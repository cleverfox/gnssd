-module(device).

-behaviour(gen_server).

%% API functions
-export([start_link/1, start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
	  id,
	  kind,
	  settings, 
	  sub_position,
	  cur_poi,
	  history_raw,
	  history_events,
	  chour,
	  data,
	  last_ptime
	 }).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(ID) ->
    gen_server:start_link({global, {device, ID}}, ?MODULE, [ID, 0], []).

start_link(ID, Hour) ->
    gen_server:start_link({global, {device, ID, Hour}}, ?MODULE, [ID, Hour], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init1(ID,Kind,Settings,Hour) ->
	{ok,PSub,TTL}=esub:device_get_sub("position",ID),
	reload_after("position",TTL),
	lager:info("Starting worker ~p ~p, sub ~p",[Kind,Settings,PSub]),
	UnixHour=case Hour of 
				 0 -> 
					 {MSec,Sec,_}=now(),
					 gpstools:floor((MSec*1000000+Sec)/3600);
				 UH when is_integer(UH) -> 
					 UH
			 end,
	PL=case mng:find_one(<<"rawdata">>,{type,rawdata,device,ID,hour,UnixHour}) of
		   {Term} when is_tuple(Term) -> 
			   mng:m2proplist(Term);
		   _ -> []
	   end,
	{LPTime, PLS, CHour, Dict}=case proplists:get_value(raw,PL) of 
							 List when is_list(List) ->
								 PLS0=[ mng:m2proplist(X) || X<-List ],
								 MaxTime=lists:foldl(fun(Arr,Ai)-> 
															 V=case proplists:get_value(dt,Arr) of 
																   X when is_integer(X) -> X; 
																   _ -> 0
															   end, 
															 case Ai < V of 
																 true -> V ; 
																 _ -> Ai 
															 end 
													 end, 0, PLS0),
								 PLS1=[ {proplists:get_value(dt,X),now(),X} || X<-PLS0 ],
								 D1=dict:store(lastdump,now(),dict:new()),
								 {MaxTime, PLS1, UnixHour, D1};
							 undefined -> 
								 {0, [], 0, dict:new()}
						 end,
	Dict2=dict:store(timeout,
			case Hour of 
				 0 -> 1800;
				 _ -> 300
			end, Dict),
	{ok, #state{last_ptime=LPTime,
				history_raw=PLS,
				history_events=[],
				chour=CHour,
				id=ID,
				kind=Kind,
				settings=Settings,
				sub_position=PSub,
				data=Dict2,
				cur_poi=[]}
	}
	
	.

init([ID]) ->
	init([ID,0]);

init([ID,Hour]) ->
	lager:info("Staring ~p",[ID]),
	case psql:equery("select kind,settings from devices where id=$1",[ID]) of
		{ok,_Hdr,[{Kind,Settings}]} ->
			init1(ID,Kind,Settings,Hour);
		_ -> 
			{error, cant_start}
	end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(getState, _From, State) ->
	{reply, State, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

f2b(X) when is_integer(X) ->
       	integer_to_binary(X);
f2b(X) when is_float(X) -> 
	float_to_binary(X,[{decimals, 20}, compact]).

handle_cast({stop, Reason}, State) ->
	S=dump_stat(State,1),
	{stop, Reason, S};

handle_cast({ds, List}, State) ->
	ds(List,State, 4);

handle_cast({ds, _, 0}, State) ->
	{noreply, State};

handle_cast({ds, List, TTL}, State) ->
	ds(List,State, TTL);

handle_cast(dump, State) ->
	{noreply, dump_stat(State,1)};

handle_cast({sub, Type}, State) ->
	{ok,PSub,TTL}=esub:device_get_sub("position",State#state.id),
	reload_after("position",TTL),
	lager:info("Worker ~p got notification ~p",[State#state.id,Type]),
	{noreply, State#state{sub_position=PSub}};

handle_cast(_Msg, State) ->
	lager:info("Worker ~p got cast ~p",[State#state.id,_Msg]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({stop, Reason}, State) ->
	S=dump_stat(State,1),
	{stop, Reason, S};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	lager:info("CodeChange from ~p, extra ~p",[_OldVsn, _Extra]),
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
reload_after(Database,TTL) ->
	lager:info("Set ~p timer to ~p",[Database,TTL]),
	gen_server:cast(self(),{ttl,Database,TTL}).

dump_stat(State) ->
	dump_stat(State, 0).

dump_stat(State, Force) ->
	LastDump=case dict:find(lastdump,State#state.data) of
				 error -> {0,0,0};
				 {ok, {_,_,_}} = {ok, DateTime} -> DateTime
			 end,
	LastSec=timer:now_diff(now(),LastDump)/1000000,
	case (Force==1) orelse (LastSec > 300) of 
		true ->
			lager:debug("Device ~p dumping to mongodb....",[State#state.id]),
			HR=case State#state.history_raw of
				   M when is_list(M) -> M;
				   _ -> []
			   end,
			%HR1 = [ list_to_tuple(lists:flatten([ tuple_to_list(S1) || S1 <- Src ])) || {_,_,Src} <- HR ],
			%
			%	lager:debug("Dump ~p",[HR1]),
			HR1=mng:proplist2m(HR),
			mng:ins_update(<<"rawdata">>,
						   {type,rawdata,
							device,State#state.id,
							hour,State#state.chour},
						   {raw,HR1}
						  ),
			State#state{data=dict:store(lastdump,now(),State#state.data)};
		false -> 
			State
	end.


switch_hour(State) ->
	St1=dump_stat(State,1),
	lager:info("Switch hour"),
	St1#state{history_raw=[]}.

process_ds(List,State) ->
	Notify=State#state.sub_position,
	{_,[Lon, Lat]}=proplists:lookup(position,List),
	{_,Dir}=proplists:lookup(dir,List),
	{_,T}=proplists:lookup(dt,List),
	{_,Speed}=proplists:lookup(sp,List),
	UnixHour=gpstools:floor(T/3600),
	PHist=case is_list(State#state.history_raw) of
			  true -> State#state.history_raw;
			  _ -> []
		  end,

	NewState=case T > State#state.last_ptime of
				 true ->
					 POIs=case psql:equery("select id from pois where ST_Intersects(geo,st_makepoint($1,$2))",[Lon,Lat]) of
							  {ok,_Hdr,Dat} ->
								  [ X || {X} <- Dat ];
							  _Any -> 
								  []
						  end,
					 OLDPois=case is_list(State#state.cur_poi) of
								 true -> State#state.cur_poi;
								 _ -> []
							 end,
					 POIIn=lists:subtract(POIs,OLDPois),
					 POIOut=lists:subtract(OLDPois,POIs),
					 In0=case proplists:lookup(in0,List) of
							 {_,Float} when is_float(Float) -> round(Float*100)/100;
							 _ -> none
						 end,
					 Log=[State#state.id,round(Lon*10000)/10000,round(Lat*10000)/10000,round(Speed),POIIn,POIOut,In0],
					 lager:info("dev ~p at ~p,~p (~p km/h) Pois ~p/~p in0: ~p",Log),
					 %lager:info("POI: ~p",[POIs]),


					 %put data into Redis
					 Bi=integer_to_binary(State#state.id),
					 DevH= <<"device:lastpos:",Bi/binary>>,
					 DevP= <<"device:cpoi:",Bi/binary>>,
					 Redis=fun(W) -> 
								   eredis:q(W, [ "hmset", DevH, 
												 "lng", f2b(Lon),
												 "lat", f2b(Lat),
												 "dir", f2b(Dir),
												 "spd", f2b(Speed),
												 "t", T ]),
								   eredis:q(W, [ "del", DevP ]),
								   eredis:q(W, [ "sadd", DevP ] ++ POIs)
						   end,
					 poolboy:transaction(redis,Redis),


					 %send data to push stream
					 Data={struct,[
								   {type,position},
								   {dev,State#state.id},
								   {dir,Dir},
								   {spd,Speed},
								   {pois, POIs},
								   {pos,{array,[Lon, Lat]}}
								  ]},
					 JSData=iolist_to_binary(mochijson2:encode(Data)),
					 %lager:info("JS: ~p",[JSData]),
					 Fd=fun(E) ->
								gen_server:cast(redis2nginx,{push,<<"push:",E/binary>>,JSData})
						end,
					 lists:foreach(Fd,Notify),

					 State#state{cur_poi=POIs, history_raw=PHist ++ [{T,now(),List}], chour=UnixHour};
				 _ ->
					 State#state{
					   history_raw= lists:sort(fun({A,_,_},{B,_,_})-> B>A end,PHist ++ [{T,now(),List}]),
					   chour=UnixHour}
			 end,
	dump_stat(NewState).

ds(List0, State, TTL) ->
	case dict:find(timer,State#state.data) of
		{ok, Timer} ->
			erlang:cancel_timer(Timer);
		_ -> ok
	end,


%	lager:info("Worker ~p got datasource ~p",[State#state.id,List0]),
	List=[ {case K of B when is_binary(B) -> list_to_atom(binary_to_list(B)); _-> K end,V} || {K,V} <- List0 ],
	{_,T}=proplists:lookup(dt,List),
	UnixHour=gpstools:floor(T/3600),
	CurH=case State#state.chour of
		M when is_integer(M) -> M;
		_ -> UnixHour
	end,
	NextH=CurH+1,
	Res=case UnixHour of
		CurH ->
			process_ds(List,State);
		NextH ->
			S1=switch_hour(State),
			process_ds(List,S1);
		_Any when _Any > NextH ->
			lager:error("Data from future!!! ~p: Current hour ~p, Data hour ~p",[State#state.id,State#state.chour, UnixHour]),
			State;
		_Any when _Any < CurH -> 
%			lager:error("Fix me: spawn new worker and do my best ~p: ~p, ~p",[State#state.id,State#state.chour, UnixHour]),
			case global:whereis_name({device, State#state.id, UnixHour}) of
				undefined -> 
					case supervisor:start_child(dev_sup,[State#state.id, UnixHour]) of
						{ok,Pid} -> 
%							lager:error("Ok, found pid  ~p",[Pid]),
							gen_server:cast(Pid,{ds, List, TTL-1});
						Any -> 
							lager:error("Can't start device: ~p",[Any])
					end;
				Pid ->
%					lager:error("Ok, found pid  ~p",[Pid]),
					gen_server:cast(Pid,{ds, List, TTL-1})
			end,
			State
	end,
	Timeo=case dict:find(timeout,State#state.data) of
			  {ok, MM} -> MM;
			  _ -> 3600
		  end,
	D2=dict:store(timer,
				  erlang:send_after(1000*Timeo, self(), {stop, normal}),
				  State#state.data),
	D3=dict:store(last_ds,now(),D2),
	{noreply, Res#state{data=D3}}.


