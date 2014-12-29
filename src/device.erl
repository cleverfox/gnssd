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
	  fixedhour, 
	  org_id,
	  kind,
	  settings, 
	  sub_position,
	  sub_ev,
	  cur_poi,
	  history_raw,
	  history_events,
	  history_processed,
	  current_values,
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
init1(ID,Kind,OID,Settings,Hour) ->
	{ok,PSub}=esub2:device_get_sub(position,ID),
	ESub=esub2:device_get_sub(ID),
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
				org_id=OID,
				fixedhour=Hour,
				kind=Kind,
				settings=Settings,
				sub_position=PSub,
				sub_ev=ESub,
				data=Dict2,
				current_values=dict:new(),
				cur_poi=[]}
	}.

init([ID]) ->
	init([ID,0]);

init([ID,Hour]) ->
	lager:info("Staring ~p",[ID]),
	case psql:equery("select kind,settings,organisation_id from devices where id=$1",[ID]) of
		{ok,_Hdr,[{Kind,Settings,OID}]} ->
			CFG=case decode_settings(Settings) of
					{ok, C} -> 
						C;
					_ -> 
						lager:error("Can't decode config for device ~p",[ID]),
						 []
				end,
			init1(ID,Kind,OID,CFG,Hour);
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

handle_cast({sub, SType}, State) ->
	Type=case SType of
			 M when is_atom(M) -> M;
			 M when is_list(M) -> list_to_atom(M);
			 M when is_binary(M) -> list_to_atom(binary_to_list(M));
			 _ -> unknown
		 end,
	{ok,PSub}=esub2:device_get_sub(position,State#state.id),
	{ok,Sub}=esub2:device_get_sub(Type,State#state.id),
	lager:debug("Worker ~p got notification ~p",[State#state.id,Type]),
	SubEV=State#state.sub_ev,
	{ESub,Found}=lists:mapfoldl(fun({K,V}, F)-> 
										case K of 
											Type ->
												{{K, Sub}, F+1};
											_ ->
												{{K,V}, F}
										end
								end, 0, SubEV),
	ESub2=case Found of
			  0 -> SubEV ++ [{Type, Sub}];
			  _ -> ESub
		  end,
	{noreply, State#state{sub_position=PSub,sub_ev=ESub2}};

handle_cast(reload_settings, State) ->
	lager:info("reload ~p",[State#state.id]),
	case psql:equery("select kind,settings from devices where id=$1",[State#state.id]) of
		{ok,_Hdr,[{Kind,Settings}]} ->
			case decode_settings(Settings) of
					{ok, C} -> 
						{noreply, State#state{settings=C,kind=Kind}};
					_ -> 
					{noreply, State}
				end;
		_ -> 
			lager:error("Can't reload settings for ~p",[State#state.id]),
			{noreply, State}
	end;

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
	%Notify=State#state.sub_position,
	Notify=case lists:keyfind(position,1,State#state.sub_ev) of
			   {position, L} when is_list(L) -> L;
			   _ -> []
		   end,
	{_,[Lon, Lat]}=proplists:lookup(position,List),
	{_,Dir}=proplists:lookup(dir,List),
	{_,T}=proplists:lookup(dt,List),
	{_,Speed}=proplists:lookup(sp,List),
	UnixHour=gpstools:floor(T/3600),
	PHist=case is_list(State#state.history_raw) of
			  true -> State#state.history_raw;
			  _ -> []
		  end,
	%PEv=case is_list(State#state.history_events) of
	%		  true -> State#state.history_events;
	%		  _ -> []
	%	  end,

	%lager:info("Config ~p",[State#state.settings]),
	%CurData=[{dt,T},
	%		 {position,[Lon, Lat]},
	%		 {dir, Dir},
	%		 {speed,Speed}],


	NewState=case T > State#state.last_ptime of
				 true ->
					 POIs=case psql:equery("select id from pois where (organisation_id=$3 or organisation_id is null) and ST_Intersects(geo,st_makepoint($1,$2))",
										   [Lon,Lat,State#state.org_id]) of
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
					 In1=case proplists:lookup(in0,List) of
							 {_,Floa} when is_float(Floa) -> round(Floa*100)/100;
							 {_,Int} when is_integer(Int) -> Int;
							 _ -> none
						 end,
					 %lager:info("POI: ~p",[POIs]),
					 %lager:info("Sp ~p, St ~p",[Speed, proplists:get_value(minspeed,State#state.settings,1)]),
					 Stop=Speed < proplists:get_value(minspeed,State#state.settings,1),
					 {LStart,LStop,LState,LProcessed} = case dict:find(startstop,State#state.data) of
										{ok, {A,B,C} } -> {A,B,C,false};
										{ok, {A,B,C,D} } -> {A,B,C,D};
										_ -> case Stop of
												 true -> {T, T, stop, false};
												 _ -> {T, T, drive, false}
											 end
									end,
					 NSSData=case {Stop == (LState == stop), Stop} of
							   {true, true} -> %Still stopped
									 case LProcessed of
										 true -> 
											 {LStart, LStop, LState, LProcessed};
										 false ->
											 MStop=proplists:get_value(minstop,State#state.settings,300),
											 case T - LStop >= MStop of
												 true -> 
													 lager:info("Car ~p Really stopped",[State#state.id]),
													 {LStart, LStop, LState, true};
												 false -> 
													 {LStart, LStop, LState, LProcessed}
											 end
									 end;
							   {true, false} -> %Still driveing
								   {LStart, LStop, LState, false};
							   {false, true} -> %stopped
									 lager:info("Stop after ~p",[T-LStart]),
								   {LStart, T, stop, false};
							   {false, false} -> %started
									 lager:info("Start after ~p",[T-LStop]),
								   {T, LStop, drive, false}
						   end,
					 lager:info("Car ~p L ~p SSData ~p",[State#state.id,LState,NSSData]),

					 Log=[State#state.id,round(Lon*10000)/10000,round(Lat*10000)/10000,round(Speed),POIIn,POIOut,In0,In1,element(3,NSSData)],
					 lager:info("dev ~p at ~p,~p (~p km/h) Pois ~p/~p in0: ~p, in1: ~p, ~p",Log),

					 %put data into Redis
					 Bi=integer_to_binary(State#state.id),
					 DevH= <<"device:lastpos:",Bi/binary>>,
					 DevP= <<"device:cpoi:",Bi/binary>>,
					 Redis=fun(W) -> 
								   eredis:q(W, [ "hmset", DevH, 
												 "lng", f2b(Lon),
												 "lat", f2b(Lat),
												 "dir", f2b(Dir),
												 "spd", case Stop of 
															true -> 0; 
															_ -> f2b(Speed) 
														end,
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
								   {color, case Speed of
											   M when M<2 -> <<"blue">>;
											   M when M<60 -> <<"green">>;
											   _ -> <<"red">>
										   end},
								   {pos,{array,[Lon, Lat]}}
								  ]},
					 JSData=iolist_to_binary(mochijson2:encode(Data)),
					 %lager:info("JS: ~p",[JSData]),
					 Fd=fun(E) ->
								%lager:info("Dev ~p Send notify ~p ~p",[State#state.id,E,JSData]),
								gen_server:cast(redis2nginx,{push,<<"push:",E/binary>>,JSData})
						end,
					 lists:foreach(Fd,Notify),

					 State#state{
					   cur_poi=POIs, 
					   history_raw=PHist ++ [{T,now(),List}], 
					   chour=UnixHour, 
					   data=dict:store(startstop,NSSData,State#state.data)
					  };
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
	Res=case State#state.fixedhour of
			0 ->
				CurH=case State#state.chour of
						 0 -> UnixHour;
						 M when is_integer(M) -> M;
						 _ -> UnixHour
					 end,
				NextH=CurH+1,
				case UnixHour of
					CurH ->
						process_ds(List,State);
					NextH ->
						S1=switch_hour(State),
						process_ds(List,S1);
					_Any when _Any > NextH ->
						lager:error("Data from future!!! ~p: Current hour ~p, Data hour ~p",[State#state.id,State#state.chour, UnixHour]),
						State;
					_Any when _Any < CurH -> 
						case global:whereis_name({device, State#state.id, UnixHour}) of
							undefined -> 
								case supervisor:start_child(dev_sup,[State#state.id, UnixHour]) of
									{ok,Pid} -> 
										gen_server:cast(Pid,{ds, List, TTL-1});
									Any -> 
										lager:error("Can't start device: ~p",[Any])
								end;
							Pid ->
								gen_server:cast(Pid,{ds, List, TTL-1})
						end,
						State
				end;

			FH when is_integer(FH) -> 
				case UnixHour of
					FH ->
						process_ds(List,State);
					_ -> 
						lager:error("It is not my ds (my fixed hour ~p:~p, but ~p come)",[State#state.id,FH,UnixHour])
				end
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


b2ia (Bin) ->
	case catch binary_to_integer(Bin) of
		X when is_integer(X) ->
			X;
		_Any -> 
			list_to_atom(binary_to_list(Bin))
	end.

%binary_to_number(B) -> list_to_number(binary_to_list(B)).
%list_to_number(L) ->
%	try list_to_float(L)
%	catch
%		error:badarg ->
%			list_to_integer(L)
%	end.

processStruct({struct, X},Path,TK) ->
	[ { b2ia(I),processStruct(P,Path++[I],TK)} || {I,P} <- X ];
processStruct(X,Path,TK) when is_list(X) ->
	[ { b2ia(I),processStruct(P,Path++[I],TK)} || {I,P} <- X ];
processStruct(X,Path,TK) ->
	case TK of
		undefined -> X;
		F ->
			F(X,Path)
	end.	

decode_settings(Settings) ->
	Fun=fun(X,Path) ->
				case Path of 
					[<<"minstop">>] -> 
						binary_to_integer(X);
					[<<"minspeed">>] -> 
						binary_to_integer(X);
					[<<"inputs">>,_,<<"type">>] -> 
						list_to_atom(binary_to_list(X));
					_ ->
						%lager:debug("Process val ~p at ~p",[X,Path]),
						X
				end
		end,
	case mochijson2:decode(Settings) of
		{struct,Arr} when is_list(Arr) ->
			P1=processStruct(Arr,[],Fun),
			lager:info("ParseSettings ~p",[P1]),
			{ok, P1};
		_ -> 
			lager:info("ParseSettings error ~p",[Settings]),
			error 
	end.

