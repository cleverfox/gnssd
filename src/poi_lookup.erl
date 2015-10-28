-module(poi_lookup).
-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,lookup/4,lookup/3,get_member/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
	pg2:create(?MODULE),
    gen_server:start_link(?MODULE, [], []).

lookup(Org,Lon,Lat) ->
	lookup(Org,Lon,Lat, 5000).

get_member() ->
	Members=lists:map(fun(Pid) ->
							  [{message_queue_len, QLen}] = erlang:process_info(Pid, [message_queue_len]),
							  {Pid, QLen}
					  end,
					  pg2:get_members(?MODULE)),
	case lists:keysort(2, Members) of
		[{Pid, _} | _] -> {ok,Pid};
		[] -> {error, nomembers}
	end.

lookup(Org,Lon,Lat,Timeout) ->
	{ok, Pid} = get_member(),
	try
		{ok,List,_Time}=gen_server:call(Pid,{lookup,Org,Lon,Lat,time_compat:erlang_system_time(micro_seconds),Timeout},Timeout),
		{ok,List}
	catch exit:{timeout,_} ->
			  {timeout,[]};
		  _Ec:_Ee ->
			  {error,[]} 
	end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
	pg2:join(poi_lookup, self()),
    {ok, #{}}.

handle_call(status, _From, State) ->
	L=lists:map(fun
					({{late,T},C}) ->
						{<<"Late ",(integer_to_binary(T*10))/binary,"-",(integer_to_binary((T+1)*10))/binary>>,C};
					({T,C}) ->
						{<<(integer_to_binary(T*5))/binary,"-",(integer_to_binary((T+1)*5))/binary>>,C}
				end,maps:to_list(State)),
	{reply, L, State};

handle_call(statusreset, _From, State) ->
	L=lists:map(fun
					({{late,T},C}) ->
						{<<"Late ",(integer_to_binary(T*10))/binary,"-",(integer_to_binary((T+1)*10))/binary>>,C};
					({T,C}) ->
						{<<(integer_to_binary(T*5))/binary,"-",(integer_to_binary((T+1)*5))/binary>>,C}
				end,maps:to_list(State)),
	{reply, L, #{}};

handle_call({lookup, Org, Lon, Lat, ReqNow, Timeout}, _From, State) ->
	%lager:info("I ~p, len ~p~n",[self(),erlang:process_info(self(), [message_queue_len])]),
	case (time_compat:erlang_system_time(micro_seconds)-ReqNow)/1000 >= Timeout of
		true ->
			%lager:info("Reqd ~p ~p too late ~n",[Timeout,timer:now_diff(now(),ReqNow)/1000]),
			{reply, {too_late,[],0}, maps:put({late,round(Timeout/10)}, maps:get({late,round(Timeout/10)},State,0)+1, State)};
		false ->
			%lager:info("Reqd ~p ~p ~n",[Timeout,timer:now_diff(now(),ReqNow)/1000]),
			SQL="select id from pois where (organisation_id = $3 or organisation_id = 0) and ST_Intersects(geo,st_makepoint($1,$2))",
			Time1=time_compat:erlang_system_time(micro_seconds),
			SQLRes=psql:equery(SQL, [Lon,Lat,Org]),
			Time2=time_compat:erlang_system_time(micro_seconds),
			TimeDiff=(Time2-Time1)/1000,

			TimeCat=round(TimeDiff/5),
			%lager:info("POI lookup took ~p ~p",[TimeDiff,[Lon,Lat,Org]]),
			if TimeDiff>500 ->
				   lager:error("POI lookup took ~p ~p",[TimeDiff,[Lon,Lat,Org]]);
			   true ->
				   ok
			end,
			POIs=case SQLRes of
					 {ok,_Hdr,Dat} ->
						 [ X || {X} <- Dat ];
					 _Any -> 
						 []
				 end,
			{reply, {ok,POIs,TimeDiff}, maps:put(TimeCat, maps:get(TimeCat,State,0)+1, State) }
	end;

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

