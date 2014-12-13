-module(device).

-behaviour(gen_server).

%% API functions
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
	  id,
	  type,
	  settings, 
	  sub_position,
	  sub_event
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
    gen_server:start_link({global, {device, ID}}, ?MODULE, [ID], []).

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
init([ID]) ->
	lager:info("Staring ~p",[ID]),
	case psql:equery("select type,settings from devices where id=$1",[ID]) of
		{ok,_Hdr,[{Type,Settings}]} ->
			{ok,PSub,TTL}=esub:device_get_sub("position",ID),
			reload_after("position",TTL),
			lager:info("Starting worker ~p ~p, sub ~p",[Type,Settings,PSub]),
			{ok, #state{id=ID, type=Type,settings=Settings,sub_position=PSub}};
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
	{stop, Reason, State};

handle_cast({ds, List}, State) ->
	%lager:info("Worker ~p got datasource ~p",[State#state.id,List]),
	Notify=State#state.sub_position,
	{_,[Lon, Lat]}=proplists:lookup(<<"position">>,List),
	{_,Dir}=proplists:lookup(<<"dir">>,List),
	{_,T}=proplists:lookup(<<"dt">>,List),
	{_,Speed}=proplists:lookup(<<"sp">>,List),
	Bi=integer_to_binary(State#state.id),
	lager:info("dev ~p at ~p,~p (~p km/h) ~p deg",[binary_to_integer(Bi),round(Lon*10000)/10000,round(Lat*10000)/10000,Speed,round(Dir*10)/10]),

	POIs=case psql:equery("select id from pois where ST_Intersects(geo,st_makepoint($1,$2))",[Lon,Lat]) of
		    {ok,_Hdr,Dat} ->
			    [ X || {X} <- Dat ];
		    _Any -> 
			    []
	    end,
	%lager:info("POI: ~p",[POIs]),

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
	{noreply, State};

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
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
reload_after(Database,TTL) ->
	lager:info("Set ~p timer to ~p",[Database,TTL]),
	gen_server:cast(self(),{ttl,Database,TTL}).

