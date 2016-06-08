-module(device).

-behaviour(gen_server).

%% API functions
-export([start_link/1, start_link/2, start_link/3, test/1, tar/2, known_atoms/0, get_init_hourstate/3, prepare_tartab/1, syncrecalc/1 ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
		 fetch_last/3]).

-include("include/device.hrl").
-include("include/usersub.hrl").

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

start_link(ID, Hour, Recalc) ->
    gen_server:start_link({global, {device, ID, Hour}}, ?MODULE, [ID, Hour, Recalc], []).

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
	init([ID,0,false]);

init([ID,Hour]) ->
	init([ID,Hour,false]);

init([ID,Hour,Recalc]) ->
	lager:info("Starting worker ~p: h ~p ~s",[ID, Hour, Recalc]),

	lager:md([{worker,device},{module,?MODULE},{device_id,ID}]),
	case Hour of
		0 -> catch register(list_to_atom("device_"++integer_to_list(ID)),self());
		_ -> ok
	end,

	case psql:equery("select kind,settings,organisation_id,imei from devices where id=$1",[ID]) of
		{ok,_Hdr,[{Kind,Settings,OID,IMEI}]} ->
			CFG=case decode_settings(Settings) of
					{ok, C} -> 
						lager:debug("Settings ~p",[C]),
						DevID=integer_to_binary(ID),
						gen_server:cast(redis_set,{cmd, [
														 <<"setex">>,
														 <<"device:config:",DevID/binary>>, 
														 86400*365,
														 term_to_binary(C,[{compressed,9}])
														] 
												  }),
						C;
					_ -> 
						lager:error("Can't decode config for device ~p",[ID]),
						 []
				end,
			if Recalc == synccfg ->
				   ok;
			   true ->
				   init_device(ID,Kind,OID,CFG,Hour,Recalc, IMEI)
			end;
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
handle_call(get_state, _From, State) ->
	{reply, State, State};

handle_call(get_path, _From, State) ->
	try
		T1=State#state.chour*3600,
		T2=(State#state.chour+1)*3600,
		Res=lists:filtermap(fun({DT,List}) when DT>=T1 andalso DT<T2 ->
									{true,[DT,proplists:get_value(position,List)]};
							   (_) ->
									false
					  end, State#state.history_processed),
		{reply, Res, State}
	catch Ec:Ee ->
			  {reply, {error, Ec, Ee}, State}
	end;

handle_call(get_latest_point, _From, State) ->
	Res=try
			{_,Point}=lists:last(State#state.history_raw),
			Point
		catch _:_ ->
				  error
		end,
	{reply, Res, State};

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


handle_cast({stop, Reason}, State) ->
	S=dump_stat(State,true),
	recalc_ifneeded(State),
	{stop, Reason, S};

handle_cast({ds, List}, State) ->
	ds(List,State, 4);

handle_cast({ds, List, 0}, State) ->
	lager:error("DS ttl exceeded for device ~p: ~p",
				[State#state.id, List]),
	{noreply, State};

handle_cast({ds, List, TTL}, State) ->
	ds(List,State, TTL);

handle_cast(dump, State) ->
	{noreply, dump_stat(State,true)};

handle_cast(needrecalc, State) ->
	{noreply, 
	 State#state{
	   data= maps:put(recalc, true, State#state.data)
	  }
	};

handle_cast({sub, SType}, State) ->
	Type=case SType of
			 M when is_atom(M) -> M;
			 M when is_list(M) -> list_to_atom(M);
			 M when is_binary(M) -> list_to_atom(binary_to_list(M));
			 _ -> unknown
		 end,
%	{ok,PSub}=esub2:device_get_sub(position,State#state.id),
	{ok,Sub}=esub2:device_get_sub(Type,State#state.id),
	lager:debug("SUB update ~p: ~p",[SType,Sub]),
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
	{noreply, State#state{
			%	sub_position=PSub,
				sub_ev=ESub2}};


handle_cast(reg, State) ->
	%register(list_to_atom("device_"++integer_to_list(State#state.id)),self()),
	{noreply, State};

handle_cast(reload_subs, State) ->
	Subs=get_event_subs(State#state.id),
	lager:info("reload ~p subs",[State#state.id]),
	lager:debug("Car ~p subs ~p",[State#state.id,Subs]),
	{noreply, State#state{ usersub=Subs }};

handle_cast(reload_settings, State) ->
	Subs=get_event_subs(State#state.id),
	case psql:equery("select kind,settings from devices where id=$1",[State#state.id]) of
		{ok,_Hdr,[{Kind,Settings}]} ->
			case decode_settings(Settings) of
				{ok, C} -> 
					lager:info("Settings ~p",[C]),
					DevID=integer_to_binary(State#state.id),
					gen_server:cast(redis_set,{cmd, [
													 <<"setex">>,
													 <<"device:config:",DevID/binary>>, 
													 86400*365,
													 term_to_binary(C,[{compressed,9}])
													] 
											  }),

					lager:info("Car ~p reload settings",[State#state.id]),
					{noreply, State#state{
								settings=C,
								kind=Kind,
								usersub=Subs
							   }};
				_ -> 
					lager:info("Car ~p can't decode settings",[State#state.id]),
					{noreply, State#state{
								usersub=Subs
							   }}
			end;
		_ -> 
			lager:error("Can't reload settings for ~p",[State#state.id]),
			{noreply, State#state{
						usersub=Subs
					   }}
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
handle_info(recalc_begin, State) ->
	S1=lists:foldl(fun({Ut,X},St) ->
						   lager:info("P1 ~p H ~p",[trunc(Ut/3600),State#state.chour]),
						   case trunc(Ut/3600)<State#state.chour of
							   true -> 
								   St;
							   _ -> 
								   process_ds(X,St, true)
						   end
				end, State, State#state.history_raw),
	S2=dump_stat(S1,true),
	{stop, normal, S2};

handle_info({stop, Reason}, State) ->
	S=dump_stat(State,true),
	recalc_ifneeded(State),
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

get_init_hourstate(ID,CHour,Fixed) ->
	DevID=integer_to_binary(ID),
	Hour=integer_to_binary(CHour),
	DevH= <<"device:hdata:",DevID/binary,":",Hour/binary>>,
	DevHp= <<"device:hdata:",DevID/binary>>,
	RF=fun(W)->
			   case eredis:q(W,[ "get", DevH ]) of
				   {ok, undefined} ->
					   if Fixed ->
							  none;
						  true ->
							  case eredis:q(W,[ "get", DevHp ]) of
								  {ok, undefined} -> none;
								  {ok, X} when is_binary(X) -> X;
								  _ -> none
							  end
					   end;
				   {ok, X} when is_binary(X) -> X;
				   _ -> none
			   end
	   end,
	case poolboy:transaction(redis,RF) of
		none -> [];
		Bin when is_binary(Bin) ->
			try 
				binary_to_term(Bin,[safe])
			catch 
				_:_ ->
					try 
						Term=binary_to_term(Bin),
						lager:error("Redis Term decoded unsafelly ~p",[Term]),
						Term
					catch _:_ -> 
							  lager:error("Can't decode redis data car ~p hour ~p",
										  [ID, CHour]),
							  []
					end
			end
	end.


init_device(ID,Kind,OID,Settings,Hour,Recalc, IMEI) ->
	ESub=case Recalc of  
			 false -> esub2:device_get_sub(ID);
			 _ -> [] %do not load subs on reload
		 end,
	Subs=case Recalc of 
			 false -> get_event_subs(ID);
			 _ -> [] %do not load subs on reload
		 end,

	UnixHour=case Hour of 
				 0 -> %current hour
					 trunc(time_compat:erlang_system_time(seconds)/3600);
				 UH when is_integer(UH) -> %old hour
					 UH
			 end,
	PrevHourData0=get_init_hourstate(ID,UnixHour-1, Recalc =/= false),
	PrevHourData=case proplists:get_value(last_ptime,PrevHourData0,undefined) of
					 XTime when (UnixHour*3600)>=XTime ->
						 PrevHourData0;
					 _ -> []
				 end,
	
	HistoryProcessed=preprocess_devicedata(ID,UnixHour,Recalc,
										   proplists:get_value(last_processed,PrevHourData,none)
										  ),
	Dict=case proplists:get_value(data,PrevHourData,#{}) of
			 HM when is_map(HM) -> HM;
			 _ -> #{}
		 end,

	{LastPointTime, ParsedRawData, CHour, Dict1}=preprocess_rawdata(ID,IMEI,UnixHour,Recalc,Dict),

	
	Dict2=maps:put(timeout,
			case Hour of 
				 0 -> 1800;
				 _ -> 300
			end, Dict1),
	Dict3= case maps:is_key(startstop, Dict2) of
			   true ->
				   Dict2;
			   false ->
				   case poolboy:transaction(redis,
											fun(W)->
													eredis:q(W,[
																"hmget",
																"device:lastpos:"++integer_to_list(ID),
																"Start",
																"Stop",
																"Status",
																"StatusHandled"
															   ]) end) of
					   {ok,[Sta,Sto,Stt,Han]} when 
							 is_binary(Sta) andalso 
							 is_binary(Sto) andalso 
							 is_binary(Stt) andalso 
							 is_binary(Han) -> 
						   maps:put(startstop, {b2i(Sta), b2i(Sto), b2a(Stt), b2a(Han) } , Dict2);
					   _ -> 
						   Dict2 
				   end
		   end,

	if Recalc==true ->
		   self() ! recalc_begin;
	   true ->
		   ok
	end,

	HistoryRaw=if Recalc==false ->
					  case proplists:get_value(last_raw,PrevHourData,none) of
						  {XlT,XlL} ->
							  case ParsedRawData of 
								  [] ->
									  [{XlT,XlL}];
								  [{XlT,_}|_] ->
									  [];
								  [_|_] ->
									  [{XlT,XlL}]
							  end;
						  _ -> []
					  end ++ ParsedRawData;
				  true ->
					  ParsedRawData
			   end,


	%lager:info("NewP ~p",[lists:map(fun({Tm,_})->Tm end,HistoryRaw)]),

	State=#state{last_ptime=if Recalc=/=false -> 0; true -> LastPointTime end,
				 history_raw=HistoryRaw,
				 history_processed=HistoryProcessed,
				 history_events=gb_trees:empty(),
				 chour=CHour,
				 id=ID,
				 org_id=OID,
				 fixedhour=Hour,
				 kind=Kind,
				 settings=Settings,
				 sub_ev=ESub,
				 data=Dict3,
				 current_values=#{},
				 plugins_data = proplists:get_value(plugins_data,PrevHourData,#{}),
				 usersub=Subs
				},
	if Recalc == sync ->
		   S1=syncrecalc(State,true),
		   dump_stat(S1,eoh),
		   done;
	   true -> 
		   {ok, State}
	end.

syncrecalc(State) ->
	syncrecalc(State,false).

syncrecalc(State, Prepared) ->
	lager:info("sync dev ~p hour ~p cnt ~p",[State#state.id,
											 State#state.chour,
											 length(State#state.history_raw)
											]),
	NewRaw = case Prepared of
				 true -> State#state.history_raw;
				 false ->
					 SortedRaw=lists:keysort(1,State#state.history_raw),
					 MinTime=case SortedRaw of
								 [{T1,_}|_] -> T1;
								 _ -> undefined
							 end,
					 prepare_raw4recalc(SortedRaw,State#state.id,State#state.chour,MinTime)
			 end,
	lists:foldl(fun({Ut,X},St) ->
						case trunc(Ut/3600)<State#state.chour of
							true -> 
								St;
							_ -> 
								process_ds(X,St, true)
						end
				end, State#state{
					   history_raw=NewRaw,
					   history_processed=[],
					   data=maps:put(recalc, false,
									 maps:put(disorder, false, State#state.data)
									)
					  }, NewRaw).


preprocess_devicedata(ID,UnixHour,Recalc,LP) ->
	PLPDs=if Recalc==false -> 
				 case mng:find_one(mongo,<<"devicedata">>,{type,devicedata,device,ID,hour,UnixHour}) of
					 {Term1} when is_tuple(Term1) -> 
						 try 
							 PLpdi=mng:m2proplist(Term1),
							 case proplists:get_value(data,PLpdi) of 
								 Listpdi when is_list(Listpdi) ->
									 PLpdS0=[ mng:m2proplist(X) || X<-Listpdi ],
									 [ {proplists:get_value(dt,X),X} || X<-PLpdS0 ];
								 undefined -> 
									 []
							 end
						 catch Ex:Ey ->
								   lager:notice("Can't parse ~p ~p",[{Ex,Ey},Term1]),
								   []
						 end;
					 _ -> [] %proplists:get_value(last_processed,HD,[])
				 end;
			 true -> []
		  end,
	case LP of
		{XpT,XpL} ->
			case PLPDs of 
				[] ->
					[{XpT,XpL}];
				[{XpT,_}|_] ->
					[];
				[_|_] ->
					[{XpT,XpL}]
			end;
		_ -> []
	end ++ PLPDs.


preprocess_rawdata(ID,IMEI,UnixHour,Recalc,Dict) ->
	SourceRawData=case mng:find_one(mongo,<<"rawdata">>,{type,rawdata,device,ID,hour,UnixHour}) of
					  {Term} when is_tuple(Term) -> 
						  mng:m2proplist(Term);
					  {} -> 
						  case mng:find_one(mongo,<<"rawdata">>,{type,rawdata,imei,IMEI,hour,UnixHour}) of
							  {Term} when is_tuple(Term) -> 
								  lager:info("Case 2 found"),
								  mng:update(mongo,<<"rawdata">>,
											 {type,rawdata,imei,IMEI,hour,UnixHour},
											 {device, ID}),
								  mng:m2proplist(Term);
							  _ -> []
						  end;

					  _ -> []
				  end,
	%lager:info("ID ~p",[mng:id2hex(proplists:get_value('_id',SourceRawData))]),
	case proplists:get_value(raw,SourceRawData) of 
		List when is_list(List) ->
			Points=[ mng:m2proplist(X) || X<-List ],
			{MinTime,MaxTime}= lists:foldl( fun(Point,{Imin,Imax})-> 

													case proplists:get_value(dt,Point) of 
														V when is_integer(V) -> 
															{
															 case Imin of 
																 undefined -> V;
																 _ when Imin > V -> V;
																 _ -> Imin 
															 end,
															 case Imax of 
																 undefined -> V;
																 _ when Imax < V -> V;
																 _ -> Imax 
															 end 
															};
														_ -> {Imin, Imax} 
													end
											end, {undefined,0}, Points ),
			History=[ {proplists:get_value(dt,X),X} || X<-Points ],

			NewHistory=if Recalc =/= false -> %recalc or sync
							  prepare_raw4recalc(History,ID,UnixHour,MinTime);
						  true ->  %no recalc
							  History
					  end,		
			D1=maps:put(lastdump,time_compat:erlang_system_time(seconds),Dict),
			{MaxTime, NewHistory, UnixHour, D1};
		undefined -> 
			{0, [], 0, Dict}
	end.

prepare_raw4recalc(History,ID,UnixHour,MinTime) ->
	StartHour=UnixHour*3600,
	EndHour=(UnixHour+1)*3600,

	PreLast=fetch_last(ID,UnixHour-1,MinTime),
	FilterFun=fun({Time,_Data}) ->
					  Time >= StartHour andalso EndHour > Time
			  end,
	case PreLast of
		{new, NewPoint} ->
			FPoints=lists:filter(FilterFun,History),
			[{proplists:get_value(dt,NewPoint),NewPoint}| FPoints];
		_ ->
			PrevPoint=find_prev_point(History, StartHour, undefined),
			case PrevPoint of
				{_,_} -> [PrevPoint|lists:filter(FilterFun,History)];
				_ -> lists:filter(FilterFun,History)
			end
	end.

b2i(X) when is_binary(X) ->
	try 
		binary_to_integer(X) 
	catch 
		error:_ -> 0 
	end.
b2a(X) when is_binary(X) ->
	list_to_atom(binary_to_list(X)). 


dump_stat(State) ->
	dump_stat(State, false).

dump_stat(State0, Force) ->
	Disorder=maps:get(disorder, State0#state.data, false),
	Recalc=maps:get(recalc, State0#state.data, false),
	State=if Disorder orelse Recalc ->
				 syncrecalc(State0,false);
			 true ->
				 State0
		  end,
	LastDump=maps:get(lastdump,State#state.data,0),
	LastSec=time_compat:erlang_system_time(seconds)-LastDump,
	case (Force =/= false) orelse (LastSec > 300) of 
		true ->
			Dev=State#state.id,
			Filename= <<"dump.",
						(integer_to_binary(Dev))/binary,
						".",
						(integer_to_binary(time_compat:os_system_time(seconds)))/binary,
						".bin">>,
			file:write_file(Filename,erlang:term_to_binary(State)),
			%lager:info("Device ~p dumping to mongodb....",[State#state.id]),
			Disorder=maps:get(disorder, State#state.data, false),
			HR=case State#state.history_raw of
				   M when is_list(M) -> 
					   if Disorder ->
							  lists:sort(fun ({A,_},{B,_})-> B>A end, M);
						  true -> M
					   end;
				   _ -> []
			   end,

			NewData1= if Force == true andalso State#state.fixedhour=/=0 ->
							 case maps:find(chlastpoint, State#state.data) of
								 {ok, true} -> % need recalc next one
									 lager:info("Need recalc next hr for ~p",[State#state.id]),
									 case HR of
										 [] ->
											 %no data
											 maps:put(chlastpoint, false, State#state.data);
										 _ ->
											 case global:whereis_name({device, State#state.id, (State#state.chour+1)}) of
												 undefined -> % no running process for next hr, enqueue
													 lager:debug("run recalc for ~p ~p in background",
																 [
																  State#state.id, 
																  (State#state.chour+1)
																 ]),
													 Bin= <<(integer_to_binary(State#state.id))/binary,":",
															(integer_to_binary((State#state.chour+1)*3600))/binary,":",
															(integer_to_binary((State#state.chour+1)*3600))/binary,":nextrecalc">>,
													 gen_server:cast(redis_set,{cmd, ["lpush","recalc",Bin]}),
													 maps:put(chlastpoint, false, State#state.data);
												 Pid -> %there is running proc for next hr
													 %gen_server:cast(Pid,{prevhpoint, lists:last(HR)})
													 lager:debug("shedule recalc for ~p ~p after proc ~p finishes ",
																 [
																  State#state.id, 
																  (State#state.chour+1),
																  Pid
																 ]),
													 gen_server:cast(Pid,needrecalc),
													 maps:put(chlastpoint, false, State#state.data)
											 end
									 end;
								 _ -> 
									 %no points appended. nothing need be recalculated
									 State#state.data
							 end;
						 true -> State#state.data
					  end,

			HR1=mng:proplist2tom(HR),
			_MngRes=mng:ins_update(mongo,<<"rawdata">>,
						   {type,rawdata,
							device,State#state.id,
							hour,State#state.chour},
						   {raw,HR1,done,1}
						  ),
			%lager:info("Save raw history ~p = ~p",[HR1,_MngRes]),
			PHR=case State#state.history_processed of
					M1 when is_list(M1) -> 
					   if Disorder ->
							  lists:sort(fun ({A,_},{B,_})-> B>A  end, M1);
						  true -> M1
					   end;
					_ -> 
						[]
				end,
			%lager:info("HD ~p",[hd(PHR)]),
			Ht1=State#state.chour*3600,
			Ht2=(State#state.chour+1)*3600,
			CLFun=fun({_,E},Acc) ->
						  DT=proplists:get_value(dt,E,0),
						  [XPos,YPos]=proplists:get_value(position,E,undefined),
						  if(DT>=Ht1 andalso DT<Ht2) ->
								Interval=trunc(DT/1800),
								case maps:get(Interval,Acc,undefined) of
									{X1a, X2a, Y1a, Y2a} ->
										{X1b, X2b} = if X1a > XPos -> {XPos, X2a};
														X2a < XPos -> {X1a, XPos};
														true -> {X1a, X2a}
													 end,
										{Y1b, Y2b} = if Y1a > YPos -> {YPos, Y2a};
														Y2a < YPos -> {Y1a, YPos};
														true -> {Y1a, Y2a}
													 end,
										maps:put(Interval,{X1b, X2b, Y1b, Y2b },Acc);
									undefined ->
										maps:put(Interval,{XPos,XPos,YPos,YPos},Acc)
								end;
							true ->
								Acc
						  end
				  end,
			CoarseLoc=try
						  ResH=lists:foldl(CLFun, #{}, PHR),
						  I1Mng=lists:map(fun({Hr,{X1a,X2a,Y1a,Y2a}}) ->
											{<<(integer_to_binary(Hr))/binary,".",
											   (integer_to_binary(State#state.id))/binary>>,
											 {x1,X1a,x2,X2a,y1,Y1a,y2,Y2a}}
											end,
									maps:to_list(ResH)),
						  I2Mng=mng:proplisttom(I1Mng),
						  LKey={type,locations,
								organisation_id,State#state.org_id,
								day,trunc(State#state.chour/24)},
						  _MngRes2=mng:ins_update(mongo,<<"locations">>,LKey,
												  I2Mng
												 )
						  %lager:info("HR1 ~p = ~p ",[LKey, _MngRes2])

					  catch 
						  Ec:Ee -> 
							  lists:map(fun(E)->
												lager:error("At ~p",[E])
										end,erlang:get_stacktrace()),
							  {Ec,Ee}
					  end,
			lager:debug("DumpCL ~p",[CoarseLoc]),
			%lager:info("Dump PHR ~p",[PHR]),
			PHR1=mng:proplist2tom(PHR),
			Time=time_compat:erlang_system_time(seconds),
			IUID=mng:ins_update(mongo,<<"devicedata">>,
						   {type,devicedata,
							device,State#state.id,
							hour,State#state.chour},
						   case Force of
							   eoh ->
								   {data,PHR1,eoh,true,lastupdate,Time};
							   _ -> 
								   {data,PHR1,lastupdate,Time}
						   end ),
			lager:debug("Car ~p InsUp ~p",[State#state.id,mng:id2hex(IUID)]),
			case Force of
				eoh ->
					JBin=iolist_to_binary(mochijson2:encode(
											[
											 {type,devicedata},
											 {device,State#state.id},
											 {hour,State#state.chour}
											])),
					gen_server:cast(redis_set,{mcmd, [
													  [ "lpush", <<"aggregate:devicedata">>, JBin ],
													  [ "publish", <<"aggregate">>, JBin ]
													 ]});	
				_ ->
					ok
			end,
			lager:debug("Car ~p dump",[State#state.id]),

			%Save hour state
			Term=[
				  {data,NewData1},
				  {plugins_data,State#state.plugins_data},
				  {last_ptime,State#state.last_ptime}
				 ] 
			++
			case State#state.history_raw of
				[] ->
					[];
				Ls when is_list(Ls) ->
					[{last_raw,lists:last(Ls)}]
			end
			++
			case State#state.history_processed of
				[] ->
					[];
				Ls when is_list(Ls) ->
					[{last_processed,lists:last(Ls)}]
			end,

			DevID=integer_to_binary(State#state.id),
			Hour=integer_to_binary(State#state.chour),
			DevH= <<"device:hdata:",DevID/binary,":",Hour/binary>>,
			if State#state.fixedhour == 0 ->
				   DevHp= <<"device:hdata:",DevID/binary>>,
				   gen_server:cast(redis_set,{mcmd,  
											  [
											   [
												"setex", DevHp, 86400*4,
												term_to_binary(Term,[{compressed,9}])
											   ],
											   [
												"setex", DevH, 7200,
												term_to_binary(Term,[{compressed,9}])
											   ] 
											  ] });
			   true -> 
				   gen_server:cast(redis_set,{cmd, [
													 "setex", DevH, 7200,
													 term_to_binary(Term,[{compressed,9}])
													]})
			end,


			State#state{data=
						maps:put(lastdump,time_compat:erlang_system_time(seconds),
								   maps:put(mngid,IUID, NewData1)
								  )
					   };
		false -> 
			State
	end.

switch_hour(State) ->
	St1=dump_stat(State,eoh),
	lager:info("Car ~p Switch hour ~p",[State#state.id,State#state.chour]),
	%{LR1,LR2,LR3}=lists:last(St1#state.history_raw),
	%Last_raw={LR1,LR2,LR3++[{prev_hour,1}]},
	Last_raw=lists:last(St1#state.history_raw),
	%{LP1,LP2,LP3}=lists:last(St1#state.history_processed),
	%Last_proc={LP1,LP2,LP3++[{prev_hour,1}]},
	Last_proc=lists:last(St1#state.history_processed),
	recalc_ifneeded(State),

	St1#state{
	  history_raw=[Last_raw],
	  history_processed=[Last_proc],
	  data= 
	  maps:put(chlastpoint, false,
				 maps:put(recalc, false,
							maps:put(disorder, false, State#state.data)
						   )
				)
	 }.

recalc_ifneeded(State) ->
	Disorder=maps:get(disorder, State#state.data, false),
	Recalc=maps:get(recalc, State#state.data, false),
	if Disorder orelse Recalc -> 
		   Bin= <<(integer_to_binary(State#state.id))/binary,":",
				   (integer_to_binary(State#state.chour*3600))/binary,":",
				   (integer_to_binary(State#state.chour*3600))/binary,":selfrecalc">>,

		   gen_server:cast(redis_set,{cmd, ["lpush","recalc",Bin]}),
		   true;
	   true ->
		   false
	end.


find_raw_place(Right,Item) -> %{NewArray, PrevItem}
	find_raw_place([], Right, Item, undefined).

find_raw_place(Left,[], Item, PrevItem) -> %{NewArray, PrevItem}
	{Left ++ [ Item ], PrevItem };

find_raw_place(Left,[Cur|Right], {Timestamp,_} = Item, PrevItem) -> %{NewArray, PrevItem}
	{T,_}=Cur,
	if T > Timestamp ->
		   {Left ++ [ Item, Cur | Right ], PrevItem };
	   T == Timestamp -> 
		   ignore;
	   true ->
		   find_raw_place(Left ++ [Cur], Right, Item, Cur)
	end;

find_raw_place(Left,[Cur|Right], {Timestamp,_,_} = Item, PrevItem) -> %{NewArray, PrevItem}
	{T,_,_}=Cur,
	if T > Timestamp ->
		   {Left ++ [ Item, Cur | Right ], PrevItem };
	   T == Timestamp -> 
		   ignore;
	   true ->
		   find_raw_place(Left ++ [Cur], Right, Item, Cur)
	end.

add_raw_item([], Item) -> %{NewArray, PrevItem}
	{[Item],undefined};
add_raw_item(Left, Item) -> 
	Li=lists:last(Left),
	{Left++[Item],Li}.

process_ds(List0,State,Recalc) ->
	try 
	List=case proplists:lookup(at,List0) of
			 {at, At} when is_integer(At) ->
				 List0;
			 _ ->
				 List0++[{at, time_compat:erlang_system_time(seconds)}]
		 end,
	{_,[Lon, Lat]}=proplists:lookup(position,List),
	{_,Dir}=proplists:lookup(dir,List),
	{_,T}=proplists:lookup(dt,List),
	{_,AT}=proplists:lookup(at,List),
	Valid=case proplists:get_value(valid,List,1) of
			  0 -> false;
			  "0" -> false;
			  <<"0">> -> false;
			  _ -> true
		  end,
	{_,Speed}=proplists:lookup(sp,List),
%	case  State#state.id < 10 of 
%		true -> 
%			lager:error("DS ~p",[List0]),
%			lager:error("T ~p",[T]);
%		false->
%		   	ok
%	end,

	if Lon == null ->
		   throw (badpos);
	   Lat == null ->
		   throw (badpos);
	   Speed == null ->
		   throw (badpos);
	   Dir == null ->
		   throw (badpos);
	   true -> 
		   ok
	end,

	UnixHour=trunc(T/3600),
	PrepData=case {Recalc,is_list(State#state.history_raw),T > State#state.last_ptime} of
				{true, true, _} -> %recalc. Do not change. 
					 %inefficient way.
					case find_raw_place(State#state.history_raw, {T-0.1,List}) of
						ignore ->
							ignore;
						{_,PVal1} ->
							{State#state.history_raw,PVal1}
					end;
				{false, false, _} ->  %New list
					{[{T,time_compat:erlang_system_time(seconds),List}],undefined};
				{false, true, true} -> %Append to end
					add_raw_item(State#state.history_raw, {T,List});
				{false, true, false} -> %Insert to middle
					find_raw_place(State#state.history_raw, {T,List})
					%lists:sort(fun({A,_,_},{B,_,_})-> B>A end,
					%			State#state.history_raw ++ [{T,now(),List}]
					%		   )
			end,
	case PrepData of
		ignore ->
			lager:debug("Device ~p ignoring dup packet T ~p",[State#state.id, T]),
			State;
		{PHist,PRaw} ->
%			lager:debug("Device ~p Item ~p ~p",[State#state.id, T,
%											proplists:lookup(rs0,List) ]),

			PrHist=case is_list(State#state.history_processed) of
					   true -> State#state.history_processed;
					   _ -> []
				   end,
			InCfg=case proplists:lookup(inputs,State#state.settings) of
					  {inputs, InLst} -> InLst;
					  _ -> []
				  end,
			{PrevAval,PRawVal,DeltaT}=case PRaw of
						undefined -> {[],[],null};
						{PTime,Prv} when is_list(Prv) -> 
									   PPVal=case lists:keyfind(PTime,1,PrHist) of
												 false -> [];
												 {_,PVl} when is_list(PVl) -> PVl;
												 {_,_,PVl} when is_list(PVl) -> PVl;
												 _Any -> 
													 lager:error("Badmatch ~p",[_Any]),
													 []
											 end,
									   {PPVal,Prv,T-PTime};
								   _Any -> 
							throw({badmatch,_Any,PRaw})
					end,

%			if State#state.id==1 ->
%				   lager:error("Id ~p",[List]),
%				   lager:error("Cf ~p",[InCfg]);
%			   true -> ok
%			end,
			SVals = if Valid ->
						   {SVals0,Errors}=process_variables(List, InCfg, PRawVal, DeltaT),
						   if is_list(Errors) andalso length(Errors)>0 ->
								  SaveDB=fun({Src,Dst,Err,Val}) ->
												 SrcL=if is_atom(Src) -> atom_to_list(Src);
														 is_binary(Src) -> binary_to_list(Src);
														 is_list(Src) -> Src;
														 true -> "Unknown"
													  end,
												 DstL=if is_atom(Dst) -> atom_to_list(Dst);
														 is_binary(Dst) -> binary_to_list(Dst);
														 is_list(Dst) -> Dst;
														 true -> "Unknown"
													  end,
												 ErrL=if is_atom(Err) -> atom_to_list(Err);
														 is_binary(Err) -> binary_to_list(Err);
														 is_list(Err) -> Err;
														 true -> "Unknown"
													  end,
												 try
													 {ok,_HDR,ErrRow}=psql:equery("select * from device_error where device_id=$1 and error_type=$2 limit 1",
																				  [State#state.id,SrcL++":"++DstL++":"++ErrL]),
													 if ErrRow == [] ->
															psql:equery("insert into device_error (device_id,error_type,first_detected,first_data) values ($1,$2,now(),$3)",
																		[
																		 State#state.id,
																		 SrcL++":"++DstL++":"++ErrL,
																		 iolist_to_binary(mochijson2:encode(
																							[
																							 {type,Err},
																							 {device,State#state.id},
																							 {ds,Src},
																							 {var,Dst},
																							 {val,Val}
																							]))
																		]),
															ok;
														true ->
															ok
													 end,
													 lager:debug("Device ~p DS error ~p ~p ~p",
																 [State#state.id,SrcL++":"++DstL++":"++ErrL,Val,ErrRow])

												 catch Ec:Ee -> 
														   lager:error("Error ~p:~p",[Ec,Ee]),
														   ok
												 end
										 end,
								  lists:foreach(SaveDB, Errors);
							  true -> ok
						   end,
						   SVals0;
					   true -> %invalid 
						   []
					end,


			SoftVars=if Valid ->
							case maps:find(lastpos,State#state.data) of % Software Odometer
								{ok, [0,0]} -> []; %skip if prev coords invalid;
								{ok, [PLon,PLat]} ->
									if Lon == 0 andalso Lat == 0 ->
										   [];
									   true ->
										   {Azimut,Distance}=gpstools:sphere_inverse({PLon,PLat},{Lon,Lat}),
										   [
											{softodometer,Distance*1000},
											{softcompass,Azimut}
										   ]
									end;
								_ -> []
							end;
						true -> %invalid
							[]
					 end,

			PrData=[{dt,T},
					{position,[Lon, Lat]},
					{dir, Dir},
					{delay, AT-T},
					{valid, Valid},
					{sp,Speed}] ++ SVals ++ SoftVars,
			%lager:info("vals ~p~n~p -> ~n~p",[DeltaT,PrevAval,PrData]),

			lager:debug("Car ~p src ~p",[State#state.id,List]),
			lager:debug("Car ~p agg ~p",[State#state.id,PrData]),

			NewState=case {Valid, T > State#state.last_ptime} of
						 {true, true} -> %this is a fresh point, process it imeediate
							 UserPIlst=lists:usort([
													{ S#usersub.pi_name, S#usersub.params } || 
													S <- State#state.usersub,
													is_atom(S#usersub.pi_name) andalso 
													S#usersub.pi_name =/= '' andalso 
													S#usersub.pi_name =/= null
												   ]),
%							 UserPI=maps:to_list(lists:foldl(
%													fun({PIn,PIp},Acc) ->
%															maps:put(PIn,maps:get(PIn,Acc,[])++[PIp],Acc)
%													end,maps:new(),UserPIlst)),
							 {UserPI2h,UserPI2l}=lists:foldl(
										fun({PIn,PIp},{Acc,LAcc}) ->
												Sep=try
														erlang:apply(PIn,separate,[])
													catch 
														_:_ -> false
													end,
												if Sep==true ->
													   {Acc,LAcc++[{PIn,PIp}]};
												   true ->
													   {maps:put(PIn,maps:get(PIn,Acc,[])++[PIp],Acc),LAcc}
												end
										end,{maps:new(),[]},UserPIlst),

							 Plugins=[{pi_poi,[]},{pi_stop,[]}] ++
							 case proplists:lookup(plugins,State#state.settings) of
								 none -> [];
								 {plugins,X} when is_list(X) -> 
									 %lager:info("dev ~p plugins ~p",[State#state.id, X]),
									 lists:filter(fun({Xi,_}) when is_atom(Xi) -> true;
													 (_) -> false
												  end, X);
								 _ -> []
							 end ++ 
							 maps:to_list(UserPI2h) ++ UserPI2l ++ 
							 if T > State#state.last_ptime -> % only if it's newest one
									[{pi_display,State#state.sub_ev}];
								true -> []
							 end,
							 %lager:info("settings ~p",[proplists:lookup(plugins,State#state.settings)]),
							 %lager:info("plugins ~p",[Plugins]),
							%lists:usort([  %run each configuration only once
							%			  {	S#usersub.pi_name, S#usersub.params } || S <- State#state.usersub, is_atom(S#usersub.pi_name) andalso pi_name =/= '' andalso pi_name =/= null
							%			 ]),


							 %lager:info("Car ~p PIP ~n PIP ~p ~n PIPh ~p ~n PIPl ~p",
							 %[ State#state.id, UserPI, maps:to_list(UserPI2h), UserPI2l ]),

							 %lager:info("Car ~p S ~p ~p, delay ~p sec",[State#state.id,proplists:lookup(plugins,State#state.settings),Plugins,AT-T]),

							 CleanHState=#{ 
							   id=> State#state.id,              %device id
							   org_id=> State#state.org_id,      %device org id
							   settings => State#state.settings, %device settings
							   data => State#state.data,         %temporary values
							   praw => PRawVal,                  %prev. raw data
							   chour => UnixHour,				 %current hour
							   pvals => PrevAval                 %prev. aggr data
							  },
							 {HState,PState}=
							 lists:foldl(
							   fun({PI, PIParam}, {HSt,PvtSt}) when is_atom(PI) ->
									   try
										   Sep=try erlang:apply(PI,separate,[])
											   catch _:_ -> false
											   end,
										   PII=if Sep -> {PI,PIParam};
												  true -> {PI}
											   end,
										   PrD=maps:get(PII,PvtSt,undefined),
										   {PvtData,HData} = 
										   erlang:apply(PI,ds_process,[
																	   PrD, 
																	   PrData, 
																	   PrHist,
																	   HSt,
																	   PIParam]),
										   lager:debug("Car ~p Run Plugin ~p(~p) = ~p", [State#state.id, PII, PIParam, HData]),
										   {
											maps:put(PI,maps:get(PI,HSt,[])++HData,HSt),
											maps:put(PII,PvtData,PvtSt)
										   }
									   catch
										   Class:CErr ->
											   lager:error("Car ~p Can't run plugin ~p(~p): ~p:~p",
														   [State#state.id,PI,PIParam, Class, CErr]),
											   lists:map(fun(E)->
																 lager:error("At ~p",[E])
														 end,erlang:get_stacktrace()),
											   {HSt,PvtSt}
									   end;
								  ({PI, _PIParam}, {HSt,PvtSt}) ->
									   lager:error("Car ~p Can't find plugin ~p", [State#state.id,PI]),
									   {HSt,PvtSt}
							   end,
							   {CleanHState, State#state.plugins_data} , Plugins), 
							 %lager:info("HS ~p",[size(term_to_binary(PState))-size(term_to_binary(PState,[{compressed,9}]))]),
							 %lager:debug("HS ~p",[PState]),

							 POIs=proplists:get_value(current_poi,maps:get(pi_poi,HState,[]),[]),
							 STOP=maps:get(pi_stop,HState,[]),
							 EPrData=[
									  %{poi, POIs},
									  {drive, proplists:get_value(status,STOP,0)}
									 ],

							 Log=[State#state.id,round(Lon*10000)/10000,round(Lat*10000)/10000,round(Speed),POIs,proplists:get_value(status,STOP,0),AT-T],
							 lager:debug("Car ~p at ~p,~p (~p km/h) Pois ~p ~p delay ~p",Log),

							 lists:foreach(fun(En)->
												   lager:debug("Car ~p event emitters ~p",[State#state.id,En])
										   end,State#state.usersub),
							 EES=[ S || S <- State#state.usersub, is_atom(S#usersub.ee_name) andalso S#usersub.ee_name =/= '' ],
							 EEhSrc=maps:get(eedata, State#state.data, #{}),
							 lager:debug("Car ~p Starting event emitters ~p~n    ~p",[State#state.id,EES,EEhSrc]),
							 EEData=lists:foldl(fun(EE,EEh) ->
														EEN=EE#usersub.ee_name,
														if EEN == null -> ok;
														   is_atom(EEN) ->
															   try
																   lager:debug("emitting ~p",[EE]),
																   Nv=EEN:emit(EE, HState, PrData, maps:get(EE#usersub.evid,EEh,undefined)),
																   maps:put(EE#usersub.evid,Nv,EEh)
															   catch
																   Class:CErr ->
																	   lager:error("Car ~p Event Emitter error ~p: ~p:~p",
																				   [State#state.id,EE, Class, CErr]),
																	   lists:map(fun(E)->
																						 lager:error("At ~p",[E])
																				 end,erlang:get_stacktrace()),
																	   EEh
															   end
														end
												end, 
												EEhSrc, EES),


							 State#state{
							   %cur_poi=POIs, 
							   last_ptime=T,
							   plugins_data=PState,
							   history_raw=PHist, % ++ [{T,now(),List}], 
							   history_processed=PrHist ++ [{T,PrData++EPrData}], 
							   chour=UnixHour,
							   data= maps:put(chlastpoint, true,
										  maps:put(eedata, EEData,
													 maps:put(lastpos, [Lon, Lat], State#state.data)
													)
										 )
							  };

						 {true, false} -> %disordered point, process late
							 lager:debug("Disordered packet ~p",[PrData]),
							 State#state{
							   history_raw=PHist, % lists:sort(fun({A,_,_},{B,_,_})-> B>A end,PHist ++ [{T,now(),List}]),
							   history_processed= lists:sort(fun 
																 ({A,_},{B,_})-> B>A;
																 ({A,_,_},{B,_,_})-> B>A 
															 end,PrHist ++ [{T,PrData}]
												   ), 
							   data= maps:put(disorder, true, State#state.data), 
							   chour=UnixHour};
						 {false, _} -> %non valid point
							 lager:debug("Disordered packet ~p",[PrData]),
							 State#state{
							   history_raw=PHist,
							   history_processed=lists:sort(fun 
																({A,_},{B,_})-> B>A;
																 ({A,_,_},{B,_,_})-> B>A 
															end,
															PrHist ++ [{T,PrData}]
														   ), 
							   chour=UnixHour
							  }
					 end,
	dump_stat(NewState)
	end
	
	catch
		throw:badpos ->
			lager:info("Car ~p Bad position in packet ~p",[State#state.id, List0]),
			dump_stat(State);
		error:{badrecord,incfg} ->
			lager:info("Car ~p reloading config due to structure changes",[State#state.id]),
			gen_server:cast(self(),reload_settings),
			State;
		GClass:GErr ->
			lager:error("Car ~p ds error: ~p:~p",
						[State#state.id, GClass, GErr]),
			lager:error("Car ~p ds list: ~p",
						[State#state.id, List0]),
			lists:map(fun(E)->
							  lager:error("At ~p",[E])
					  end,erlang:get_stacktrace()),
			dump_stat(State)
	end.


ds(List0, State, TTL) ->
	case maps:find(timer,State#state.data) of
		{ok, Timer} ->
			erlang:cancel_timer(Timer);
		_ -> ok
	end,

	%	lager:info("Worker ~p got datasource ~p",[State#state.id,List0]),
	List=[ {case K of B when is_binary(B) -> list_to_atom(binary_to_list(B)); _-> K end,V} || {K,V} <- List0 ],
	{_,T}=proplists:lookup(dt,List),
	UnixHour=trunc(T/3600),
	Res=case State#state.fixedhour of
			0 ->
				CurH=case State#state.chour of
						 0 -> UnixHour;
						 M when is_integer(M) -> M;
						 _ -> UnixHour
					 end,
				NextH=CurH+1,
				UnixTime=time_compat:erlang_system_time(seconds),
				NextUnixHour=trunc(UnixTime/3600),

				case UnixHour of
					0 -> %invalid
						lager:debug("Invalid data from car ~p: ~p",[State#state.id, List]),
						State;
					CurH -> %current hour
						process_ds(List,State,false);
					NextH -> %next hour
						S1=switch_hour(State),
						process_ds(List,S1,false);
					_Any when _Any > NextUnixHour -> %data from future: incorrect time from device. ignore it.
						PN={trunc(T/1000000),T rem 1000000,0},
						PLT=calendar:now_to_local_time(PN),
						lager:error("Device ~p Time from future ~p !!! Current hour ~p, Data hour ~p, next ~p",
									[State#state.id, PLT, State#state.chour, UnixHour, NextUnixHour]),
						State;
					_Any when _Any < CurH -> %data from past. spawn new worker
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
						State;
					Hour when Hour>CurH -> %it is time to switch hour, but looks like time jump at device
						S1=switch_hour(State),
						process_ds(List,S1,false)
				end;

			FH when is_integer(FH) -> 
				case UnixHour of
					FH ->
						process_ds(List,State,false);
					_ -> 
						lager:error("It is not my ds (my fixed hour ~p:~p, but ~p come)",[State#state.id,FH,UnixHour])
				end
		end,
	D2=maps:put(timer,
				  erlang:send_after(
					maps:get(timeout,State#state.data, 3600)*1000,
					self(), 
					{stop, normal}
				   ),
				  Res#state.data),
	D3=maps:put(last_ds,time_compat:erlang_system_time(seconds),D2),
	{noreply, Res#state{data=D3}}.


b2ia (Bin) when is_binary(Bin) ->
	case catch binary_to_integer(Bin) of
		X when is_integer(X) ->
			X;
		_Any -> 
			case catch binary_to_float(Bin) of
				X when is_float(X) ->
					X;
				_ ->
					L=binary_to_list(Bin),
					try 
						list_to_existing_atom(L)
					catch _:_ ->
							  L
					end
			end
	end;
b2ia (Bin) when is_atom(Bin) ->
	Bin;

b2ia (Bin) ->
	lager:info("ERROR ~p",[Bin]),
	Bin.

processStruct({struct, X},Path,TK, TK2) ->
	processStruct(X, Path, TK, TK2);
processStruct(X,Path,TK, TK2) when is_list(X) ->
	Fun = fun(IP) ->
				  %lager:debug("Proc ~p",[IP]),
				  case IP of 
					  {I,P} ->
						  %lager:info("{I,P}: {~p  , ~p}",[I,P]),
						  if is_function(TK2) -> 
								 PS=processStruct(P,Path++[I],TK, TK2),
								 TKR=TK2(b2ia(I), PS, Path),
								 %lager:info("F(~p, ~p, ~p)=~p",[b2ia(I),PS, Path,TKR]),
								 TKR;
							 true -> 
								 { b2ia(I),processStruct(P,Path++[I],TK, TK2)}
						  end;
					  A when is_list(A) ->
						  A;
					  A when is_atom(A) ->
						  A;
					  A when is_integer(A) ->
						  A;
					  A when is_float(A) ->
						  A;
					  A when is_binary(A) ->
						  b2ia(A)
				  end
		  end,
	lists:map(Fun, X);
processStruct(A,Path,TK, _) ->
	%lager:debug("Proc2 ~p ~p",[X,TK]),
	case TK of
		undefined -> A;
		{auto,C} ->
			if
				is_list(A) -> A;
				is_atom(A) -> A;
				is_integer(A) -> A;
				is_float(A) -> A;
				is_binary(A) ->
					case catch binary_to_integer(A) of
						X when is_integer(X) -> X;
						_ -> 
							case catch binary_to_float(A) of
								X when is_float(X) ->
									X;
								_ ->
									L=binary_to_list(A),
									case C of
										bin ->
											A;
										eatom ->
											try 
												list_to_existing_atom(L)
											catch _:_ ->
													  L
											end;
										atom ->
												list_to_atom(L);
										list->
												L
									end
							end
					end
			end;
		F -> F(A,Path)
	end.	

decode_settings(Settings) ->
	Fun=fun(X,Path) ->
				%lager:debug("Process val ~p at ~p",[X,Path]),
				case Path of 
					[<<"minstop">>] -> 
						binary_to_integer(X);
					[<<"minspeed">>] -> 
						binary_to_integer(X);
					[<<"inputs">>,_,<<"type">>] -> 
						list_to_atom(binary_to_list(X));
					_ ->
						X
				end
		end,
	Fun2=fun(K,X,Path) ->
				 %lager:debug("Proc ~p k ~p decode: ~p",[Path,K,X]),
				 case {Path, K} of 
					 {[], plugins} ->
						 X1=lists:map(fun([Name,Args]) ->
											  lager:debug("Proc ~p Args: ~p",[Name,Args]),
											  N1=binary_to_list(Name),
											  AName=case catch list_to_existing_atom(N1) of
												  N2 when is_atom(N2) -> N2;
												  _ -> N1
											  end,
											  {AName,processStruct(Args,Path,undefined, undefined)};
										 ([Name]) ->
											  N1=binary_to_list(Name),
											  lager:debug("Proc ~p noArgs",[N1]),
											  AName=case catch list_to_existing_atom(N1) of
														N2 when is_atom(N2) -> N2;
														_ -> N1
													end,
											  {AName,[]}
   									  end,X),
						 {K,X1};
					 {[<<"inputs">>], struct} ->
						 %lager:info("Proc ~p ~p decode: ~p",[Path,K,X]),
						 Type=proplists:get_value(type,X,gauge),
						 Fact=proplists:get_value(factor,X,1),
						 Ovf=proplists:get_value(ovfval,X,65536),
						 InB=proplists:get_value(in,X,<<"in0">>),
						 In=binary_to_list(InB),
						 Var=proplists:get_value(variable,X,InB),
						 Tab=proplists:get_value(table,X,1),
						 Lim=case proplists:get_value(limitsd,X,[null, null]) of
								 [null,null] ->
									 case proplists:get_value(limits,X,[null,null]) of
										 [null, null] -> 
											 undefined;
										 [A,B] when 
											   (is_integer(A) orelse is_float(A) orelse A==null) andalso
											   (is_integer(B) orelse is_float(B) orelse B==null) ->
											 {abs,A,B}
									 end;
								 [A,B] when 
									   (is_integer(A) orelse is_float(A) orelse A==null) andalso
									   (is_integer(B) orelse is_float(B) orelse B==null) ->
									 {delta,A,B}
							 end,
						 %{in, K, X};
						 Type1=case Type of
								   bin ->
									   Bi=proplists:get_value(bits,X,1),
									   Of=proplists:get_value(offset,X,0),
									   {bin,Of,Bi};
								   ibutton ->
									   Bi=proplists:get_value(bits,X,64),
									   Of=proplists:get_value(offset,X,0),
									   {ibutton,Of,Bi};
								   counter ->
									   {counter, Ovf};
								   _ -> 
									   Type
							   end,
						 #incfg{dsname=list_to_atom(In),
								type=Type1,
								factor=case Fact of
										   _ when is_integer(Fact) ->
											   Fact;
										   _ when is_float(Fact) ->
											   Fact;
										   null ->
											   case Tab of
												   1 -> 1;
												   _ when is_list(Tab) ->
													   try
														   prepare_tartab(Tab)
													   catch Tc:Te ->
																 lager:error("Can't prepare tartab ~p:~p ~p",
																			 [Tc,Te,Tab]),
																 0
													   end
											   end;
										   _ -> 1
									   end,
								limits=Lim,
								variable=list_to_atom("v_"++binary_to_list(Var))
								%, ovfval=Ovf
							   };

					 {[<<"inputs">>],Num} when is_integer(Num) -> %deprecated
						 lager:info("Deprecated format"),
						 Type=proplists:get_value(type,X,gauge),
						 Fact=proplists:get_value(factor,X,1),
						 Ovf=proplists:get_value(ovfval,X,4294967296),
						 In="in"++integer_to_list(Num),
						 Var=proplists:get_value(variable,X,list_to_binary(In)),
						 Tab=proplists:get_value(table,X,1),
						 %{in, K, X};
						 Type1=case Type of
								   bin ->
									   Bi=proplists:get_value(bits,X,1),
									   Of=proplists:get_value(offset,X,0),
									   {bin,Of,Bi};
								   counter ->
									   {counter, Ovf};
								   _ -> 
									   Type
							   end,
						 #incfg{dsname=list_to_atom(In),
								type=Type1,
								factor=case Fact of
										   _ when is_integer(Fact) ->
											   Fact;
										   _ when is_float(Fact) ->
											   Fact;
										   null ->
											   case Tab of
												   1 -> 1;
												   _ when is_list(Tab) ->
													   prepare_tartab(Tab)
											   end;
										   _ -> 1
									   end,
								variable=list_to_atom("v_"++binary_to_list(Var))
								%, ovfval=Ovf
								};
					 _ -> {K,X}
				 end
		 end,
	case mochijson2:decode(Settings) of
		{struct,Arr} when is_list(Arr) ->
			%lager:info("ParseSettings1 ~p",[Arr]),
			P1=processStruct(Arr,[],Fun,Fun2),
			%lager:info("ParseSettings2 ~p",[P1]),
			{ok, P1};
		_ -> 
			lager:info("ParseSettings error ~p",[Settings]),
			error 
	end.


tar([],_Val) ->
	overflow;
tar([{A1,A2,B1,K}|R],Val) ->
	if 
		Val < A1 -> underflow;
		Val == A1 -> B1;
		Val > A1 andalso A2>=Val ->
			((Val-A1)*K)+B1;
		true ->
			tar(R,Val)
	end.
	
prepare_tartab([])  -> []; %impossible
prepare_tartab([_]) -> [];
prepare_tartab([[A1,B1],[A2,B2]|R]) ->
	case A2-A1 of
		0 ->
			[];
		Td ->
			K=(B2-B1)/Td,
			[{A1,A2,B1,K}] 
	end ++ prepare_tartab([[A2,B2]|R]).



process_variables1(_List, _Pre, [], Acc, Errors, _Dt) ->
	{Acc, Errors};

process_variables1(List, PreRaw, [X|Rest], Acc, Errors, Dt) ->
	try
		case proplists:get_value(X#incfg.dsname,List) of
			undefined -> 
				process_variables1(List, PreRaw, Rest, Acc, Errors, Dt);
			Val0 ->
				Val=case X#incfg.type of
						{counter, Limit} ->
							PVal0=proplists:get_value(X#incfg.dsname,PreRaw,undefined),
							CntrDiff=process_counter(PVal0,Val0,Limit),
							CntrRes=case X#incfg.limits of
										undefined -> CntrDiff;
										{delta,Min,Max} ->
											if Dt>0 ->
												   if (Min==null orelse CntrDiff >= Min*Dt) andalso 
													  (Max==null orelse Max*Dt >= CntrDiff) ->
														  CntrDiff;
													  true ->
														  null
												   end;
											   true ->
												   CntrDiff
											end;
										_ -> CntrDiff
									end,
							lager:debug("Process counter ~p/~p ~p-~p ~p ~p -> ~p",
										[X#incfg.dsname, Dt, Val0, PVal0, CntrDiff, X#incfg.limits,CntrRes]),
							CntrRes;
						{bin, Off, Bits} ->
							case Val0 of
								null -> null;
								false -> null;
								_ when is_integer(Val0) ->
									(Val0 bsr Off) band ((1 bsl Bits)-1);
								_ -> 
									lager:notice("Invalid data for bin: ~p",[Val0]),
									null
							end;
						{ibutton, Off, Bits} ->
							case Val0 of
								null -> null;
								false -> null;
								_ when is_integer(Val0) ->
									%lager:info("IButton ~p ~p",[X,(Val0 bsr Off) band ((1 bsl Bits)-1)]),
									(Val0 bsr Off) band ((1 bsl Bits)-1);
								_ -> 
									lager:notice("Invalid data for ibutton: ~p",[Val0]),
									null
							end;
						gauge ->

							%lager:info("~p (~p) limit ~p = ~p ",
							%		   [X#incfg.dsname,X#incfg.type,X#incfg.limits,Val0]),
							case X#incfg.limits of
								undefined -> Val0;
								{abs,Min,Max} ->
									if (Min==null orelse Val0 >= Min) andalso 
									   (Max==null orelse Max >= Val0 ) ->
										   Val0;
									   true ->
										   null
									end;
								_ ->
									Val0
							end;
						_ ->
							lager:info("Type ~p",[X#incfg.type]),
							Val0
					end,

				{VFVal,Error}=case Val of
								  null ->
									  {null,[]};
								  _ when is_integer(Val) orelse is_float(Val) -> 
									  case X#incfg.factor of
										  1 -> {Val,[]};
										  undefined -> {Val,[]};
										  L when is_list(L) ->
											  %lager:debug("Input ~w tar ~p",[X#incfg.dsname,L]),
											  case tar(L,Val) of
												  overflow -> {null,[{X#incfg.dsname,X#incfg.variable,tar_overflow,Val}]};
												  underflow -> {0,[{X#incfg.dsname,X#incfg.variable,tar_underflow,Val}]};
												  Xi when is_integer(Xi) -> {Xi, []};
												  Xi when is_float(Xi) -> {Xi, []}
											  end;
										  Factor when is_integer(Factor) -> {Val * Factor, []};
										  Factor when is_float(Factor) -> {Val * Factor,[]};
										  _Any -> 
											  lager:info("Factor ~p",[_Any]),
											  {Val, [{X#incfg.dsname,X#incfg.variable,bad_factor,_Any}]}
									  end;
								  _ ->
									  {null,
									   [{X#incfg.dsname,X#incfg.variable,var_bad,Val}] 
									  }
							  end,

				NewAcc=
				case lists:keyfind(X#incfg.variable, 1, Acc) of
					false ->
						lists:keystore(X#incfg.variable, 1, Acc, 
									   { X#incfg.variable, VFVal });
					{_Name,PreVal0} ->
						PreVal = if is_integer(PreVal0) ->
										PreVal0;
									is_float(PreVal0) ->
										PreVal0;
									true -> 
										0
								 end,
						lists:keystore(X#incfg.variable, 1, Acc, 
									   { X#incfg.variable, PreVal+VFVal })
				end,
				%lager:info("Acc ~p,~n NewAcc ~p",[Acc,NewAcc]),
				%{bin,Offset,Len} -> 
				%lager:info("Binary ~p offset ~p len ~p",
				%		[X#incfg.dsname, Offset, Len]),
				%{0 ,{X#incfg.dsname,0,0}}
				%lager:debug("inCfg ~p Acc ~p",[X, NewAcc]),
				%
				process_variables1(List, PreRaw, Rest, NewAcc, Error++Errors, Dt)
		end

	catch Ec:Ee ->
			  lager:error("Error in process_variable ~p: ~p:~p (source data ~p)",[X,Ec,Ee,List]),
			  process_variables1(List, PreRaw, Rest, Acc, Errors, Dt)
	end.


process_variables(List, InCfg, PreVar, Dt) -> % {SVals,SOfvs}.
	process_variables1(List, PreVar, InCfg, [], [], Dt).

process_counter(undefined, _V2, _Limit) ->
	0;
process_counter(V1, V2, Limit) ->
	case V2-V1 of
		X when X>=0 -> X;
		_ -> Limit-V1+V2
	end.

get_event_subs(ID) ->
	try 
		{ok,_Hdr,RawSubs}=psql:equery("select d.id,u.id,u.personal_channel,e.name,e.plugin_name,e.eename,params,severity from device_events d left join events e on e.id=event_id left join users u on u.id=user_id where device_id=$1",[ID]),
		lists:filtermap(fun({EvID,Uid,UPid,EvName,PiName,EeName,UParams,Sev}) -> 
								B2EA=fun(Bin) when is_binary(Bin) ->
											 BL=binary_to_list(Bin),
											 try 
												 list_to_existing_atom(BL)
											 catch
												 _:_ ->
													 BL
											 end;
										(null) ->
											 throw(null);
										(_Any) ->
											 lager:error("I can't decode binary ~p",[_Any]),
											 throw(null)
									 end,
								try 
									case mochijson2:decode(UParams) of
										{struct,Arr} when is_list(Arr) ->
											UP=processStruct(Arr,[],{auto,bin},undefined),
											{true,#usersub{
													 evid=EvID,
													 user_id=Uid,
													 user_chan=UPid,
													 ev_name=EvName,
													 pi_name=B2EA(PiName),
													 ee_name=B2EA(EeName),
													 params=UP,
													 severity=Sev
													}}
									end
								catch 
									throw:null ->
										false;
									Class:Error -> 
										lager:info("ParseEvSub error ~p:~p at ~p",[Class,Error,erlang:get_stacktrace()]),
										false
								end
						end,RawSubs)
	catch Class:Error ->
			  lager:error("Can't parse subs ~p:~p",[Class,Error]),
			  []
	end.

known_atoms() ->
	[
	 aggregators_autorun,
	 aggregator_config,
	 aggregators_alias
	].

fetch_last(DeviceID,Hour,MyLast) ->
	case global:whereis_name({device, DeviceID, Hour}) of
		undefined ->
			lager:debug("requesting new point for ~p ~p my last ~p",[DeviceID,Hour,MyLast]),
			try
				{PL}=mng:find_one(mongo,<<"rawdata">>,{type,rawdata,device,DeviceID,hour,Hour}),
				Raw=mng:m2proplist(lists:last( proplists:get_value(raw, mng:m2proplist(PL)))),
				case proplists:get_value(dt, Raw) > MyLast of 
					true ->
						lager:debug("found new point for ~p ~p ~p",[DeviceID,Hour,Raw]),
						{new, Raw };
					false ->
						lager:debug("not found new point for ~p ~p ~p ~p",[DeviceID,Hour,MyLast,proplists:get_value(dt, Raw)]),
						nonew
				end
			catch _:_ ->
					  error
			end;
		PID ->
			case gen_server:call(PID, get_latest_point) of
				Raw when is_list(Raw) ->
					lager:debug("Got latest ~p directly from worker ~p",[Raw,PID]),
					case proplists:get_value(dt, Raw) > MyLast of 
						true ->
							{new, Raw};
						false ->
							nonew
					end;
				_Any ->
					error
			end
	end.

find_prev_point([],_SOH,LastFound) ->
	LastFound;

find_prev_point([{UT,_CP}|_Rest],SOH,LastFound) when UT >= SOH ->
	LastFound;

find_prev_point([{UT,CP}|Rest],SOH,_LastFound) ->
	find_prev_point(Rest,SOH,{UT,CP}).


%%% =====[ TEST ]======
test(counter) ->
	lists:map(fun({V1,V2,L}) ->
					  P=process_counter(V1,V2,L),
					  {V1,V2,L,P}
			  end,
	[{10,20,65536},{50,1000,65536},{65500,100,65536},{10000,100,65536}]);

test(tar) ->
	prepare_tartab([[9,0], [220,10], [650,20], [1070,30], [1480,40], [2309,60], [2715,70], [3117,80], [3540,90], [3786,95], [4095,100]]);


test(sorter) ->
	Src=[{100,now,[]}, {300,now,[]}, {500,now,[]}, {700,now,[]}],
	io:format("Src ~p~n",[Src]),
	Test1=fun() ->
				  io:format("Test1~n"),
				  Dst=find_raw_place(Src, {400,now,[]}),
				  io:format("Dst ~p~n",[Dst])
		  end,
	Test2=fun() ->
				  io:format("Test2~n"),
				  Dst=find_raw_place(Src, {800,now,[]}),
				  io:format("Dst ~p~n",[Dst])
		  end,
	Test3=fun() ->
				  io:format("Test3~n"),
				  Dst=add_raw_item(Src, {800,now,[]}),
				  io:format("Dst ~p~n",[Dst])
		  end,
	Test1(),
	Test2(),
	Test3(),
	ok;

test(complex) ->
	Dict=#{timeout => 5},
	TXTSetting="{ \"inputs\":[ { \"in\":\"in0\", \"type\":\"gauge\", \"factor\":1, \"variable\":\"voltage\" }, { \"in\":\"in1\", \"type\":\"gauge\", \"factor\":1, \"variable\":\"battery\" }, { \"in\":\"in4\", \"type\":\"counter\", \"factor\":0.01, \"variable\":\"tfuel\", \"ovfval\":65536 }, { \"in\":\"in5\", \"type\":\"counter\", \"factor\":-0.01, \"variable\":\"tfuel\", \"ovfval\":65536 } ] } ",
	Settings=decode_settings(TXTSetting),

	State=#state{last_ptime=0,
				 history_raw=[],
				 history_processed=[],
				 history_events=gb_trees:empty(),
				 chour=1000,
				 id=100500,
				 org_id=1,
				 fixedhour=0,
				 kind=undefined,
				 settings=Settings,
				 sub_ev=[],
				 data=Dict,
				 current_values=#{},
				 plugins_data=#{},
				 usersub=[]
				},
	{ok, State}.



