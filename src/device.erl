-module(device).

-behaviour(gen_server).

%% API functions
-export([start_link/1, start_link/2, start_link/3, test/1, tar/2, known_atoms/0 ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

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


init1(ID,Kind,OID,Settings,Hour,Recalc) ->
	{ok,PSub}=esub2:device_get_sub(position,ID),
	ESub=esub2:device_get_sub(ID),
	lager:info("Starting worker ~p: ~p, sub ~p",[ID, Kind, PSub]),
	Subs=get_event_subs(ID),
	lager:info("Sub ~p",[Subs]),

	UnixHour=case Hour of 
				 0 -> 
					 {MSec,Sec,_}=now(),
					 gpstools:floor((MSec*1000000+Sec)/3600);
				 UH when is_integer(UH) -> 
					 UH
			 end,
	PL=case mng:find_one(mongo,<<"rawdata">>,{type,rawdata,device,ID,hour,UnixHour}) of
		   {Term} when is_tuple(Term) -> 
			   mng:m2proplist(Term);
		   _ -> []
	   end,
	PLPD=if not Recalc -> 
				case mng:find_one(mongo,<<"devicedata">>,{type,devicedata,device,ID,hour,UnixHour}) of
					{Term1} when is_tuple(Term1) -> 
						try 
							PLpdi=mng:m2proplist(Term1),
							case proplists:get_value(data,PLpdi) of 
								Listpdi when is_list(Listpdi) ->
									PLpdS0=[ mng:m2proplist(X) || X<-Listpdi ],
									[ {proplists:get_value(dt,X),now(),X} || X<-PLpdS0 ];
								undefined -> 
									[]
							end
						catch Ex:Ey ->
								  lager:notice("Can't parse ~p",[{Ex,Ey}]),
								  []
						end;
					_ -> []
				end;
			true -> []
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
	Dict3=case poolboy:transaction(redis,
							 fun(W)->
									 eredis:q(W,[
												 "hmget",
												 "device:lastpos:"++integer_to_list(ID),
												 "Start",
												 "Stop",
												 "Status",
												 "StatusHandled"
												]) end) of
			  {ok,[Sta,Sto,Stt,Han]} when is_binary(Sta) andalso is_binary(Sto) andalso is_binary(Stt) andalso is_binary(Han) -> 
				  dict:store(startstop, {b2i(Sta), b2i(Sto), b2a(Stt), b2a(Han) } , Dict2);
			  _ -> 
				  Dict2 
			end,

	%lager:info("Standup ~p",[dict:to_list(Dict3)]),
	if Recalc ->
		   self() ! recalc_begin;
	   true ->
		   ok
	end,

	{ok, #state{last_ptime=if Recalc -> 0; true -> LPTime end,
				history_raw=PLS,
				history_processed=PLPD,
				history_events=gb_trees:empty(),
				chour=CHour,
				id=ID,
				org_id=OID,
				fixedhour=Hour,
				kind=Kind,
				settings=Settings,
				sub_position=PSub,
				sub_ev=ESub,
				data=Dict3,
				current_values=dict:new(),
				cur_poi=[],
				usersub=Subs
			   }
	}.

init([ID]) ->
	init([ID,0,false]);

init([ID,Hour]) ->
	init([ID,Hour,false]);

init([ID,Hour,Recalc]) ->
	lager:info("Staring ~p",[ID]),
	lager:md([{worker,device},{module,?MODULE},{device_id,ID}]),
	case Hour of
		0 -> catch register(list_to_atom("device_"++integer_to_list(ID)),self());
		_ -> ok
	end,

	case psql:equery("select kind,settings,organisation_id from devices where id=$1",[ID]) of
		{ok,_Hdr,[{Kind,Settings,OID}]} ->
			CFG=case decode_settings(Settings) of
					{ok, C} -> 
						lager:debug("Settings ~p",[C]),
						DevID=integer_to_binary(ID),
						FW=fun(W)->
								   eredis:q(W,[
											   "setex", <<"device:config:",DevID/binary>>, 86400*365,
											   term_to_binary(C,[{compressed,9}])
											  ]) 
						   end,
						poolboy:transaction(redis,FW),

						C;
					_ -> 
						lager:error("Can't decode config for device ~p",[ID]),
						 []
				end,
			init1(ID,Kind,OID,CFG,Hour,Recalc);
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

b2i(X) when is_binary(X) ->
	try 
		binary_to_integer(X) 
	catch 
		error:_ -> 0 
	end.
b2a(X) when is_binary(X) ->
	list_to_atom(binary_to_list(X)). 

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
					FW=fun(W)->
							   eredis:q(W,[
										   "setex", <<"device:config:",DevID/binary>>, 86400*365,
										   term_to_binary(C,[{compressed,9}])
										  ]) 
					   end,
					poolboy:transaction(redis,FW),

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
	S1=lists:foldl(fun({_Ut,_Gt,X},St) ->
%						   lager:debug("PDS: ~p",[X]),
						   process_ds(X,St, true)
				end, State, State#state.history_raw),
	S2=dump_stat(S1,1),
	{stop, normal, S2};

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
	case (Force>0) orelse (LastSec > 300) of 
		true ->
			lager:debug("Device ~p dumping to mongodb....",[State#state.id]),
			HR=case State#state.history_raw of
				   M when is_list(M) -> M;
				   _ -> []
			   end,
			HR1=mng:proplist3tom(HR),
			mng:ins_update(mongo,<<"rawdata">>,
						   {type,rawdata,
							device,State#state.id,
							hour,State#state.chour},
						   {raw,HR1}
						  ),

			PHR=case State#state.history_processed of
				   M1 when is_list(M1) -> M1;
				   _ -> []
			   end,
			%lager:info("Dump PHR ~p",[PHR]),
			PHR1=mng:proplist2tom(PHR),
			{MSec,Sec,_} = now(),
			Time=MSec*1000000 + Sec,
			IUID=mng:ins_update(mongo,<<"devicedata">>,
						   {type,devicedata,
							device,State#state.id,
							hour,State#state.chour},
						   case Force == 2 of
							   true ->
								   {data,PHR1,eoh,true,lastupdate,Time};
							   _ -> 
								   {data,PHR1,lastupdate,Time}
						   end ),
			lager:info("Car ~p InsUp ~p",[State#state.id,mng:id2hex(IUID)]),
			case Force > 1 of
				true ->
					Redis=fun(W) -> 
								  JBin=iolist_to_binary(mochijson2:encode(
														  [
														   {type,devicedata},
														   {device,State#state.id},
														   {hour,State#state.chour}
														  ])),
								  eredis:q(W, [ "lpush", <<"aggregate:devicedata">>, JBin ]),
								  eredis:q(W, [ "publish", <<"aggregate">>, JBin ])
						  end,
					poolboy:transaction(redis,Redis);
				_ ->
					ok
			end,
			lager:info("Car ~p dump",[State#state.id]),
			if State#state.fixedhour == 0 ->
				   DevID=integer_to_binary(State#state.id),
				   DevH= <<"device:pdata:",DevID/binary>>,
				   FW=fun(W)->
							  eredis:q(W,[
										  "setex", DevH, 86400*4,
										  term_to_binary(State#state.plugins_data,[{compressed,9}])
										 ]) 
					  end,
				   poolboy:transaction(redis,FW),
				   ok;
			   true -> 
				   ok
			end,




			State#state{data=dict:store(lastdump,now(),
										dict:store(mngid,IUID,
												   State#state.data
												  )
									   )
					   };
		false -> 
			State
	end.

switch_hour(State) ->
	St1=dump_stat(State,2),
	lager:info("Car ~p Switch hour ~p",[State#state.id,State#state.chour]),
	%{LR1,LR2,LR3}=lists:last(St1#state.history_raw),
	%Last_raw={LR1,LR2,LR3++[{prev_hour,1}]},
	Last_raw=lists:last(St1#state.history_raw),
	%{LP1,LP2,LP3}=lists:last(St1#state.history_processed),
	%Last_proc={LP1,LP2,LP3++[{prev_hour,1}]},
	Last_proc=lists:last(St1#state.history_processed),
	St1#state{
	  history_raw=[Last_raw],
	  history_processed=[Last_proc],
	  data= dict:store(disorder, false, State#state.data)
	 }.


find_raw_place(Right,Item) -> %{NewArray, PrevItem}
	find_raw_place([], Right, Item, undefined).

find_raw_place(Left,[], Item, PrevItem) -> %{NewArray, PrevItem}
	{Left ++ [ Item ], PrevItem };

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

process_ds(List,State) ->
	process_ds(List,State,false).

process_ds(List0,State,Recalc) ->
	List=case proplists:lookup(at,List0) of
			 {at, At} when is_integer(At) ->
				 List0;
			 _ ->
				 {MSec,Sec,_}=now(),
				 UnixTime=(MSec*1000000+Sec),
				 List0++[{at, UnixTime}]
		 end,
	{_,[Lon, Lat]}=proplists:lookup(position,List),
	{_,Dir}=proplists:lookup(dir,List),
	{_,T}=proplists:lookup(dt,List),
	{_,AT}=proplists:lookup(at,List),
	{_,Speed}=proplists:lookup(sp,List),
	UnixHour=gpstools:floor(T/3600),
	PrepData=case {Recalc,is_list(State#state.history_raw),T > State#state.last_ptime} of
				{true, true, _} -> %recalc. Do not change. 
					 %inefficient way.
					case find_raw_place(State#state.history_raw, {T-0.1,now(),List}) of
						ignore ->
							ignore;
						{_,PVal1} ->
							{State#state.history_raw,PVal1}
					end;
				{false, false, _} ->  %New list
					{[{T,now(),List}],undefined};
				{false, true, true} -> %Append to end
					add_raw_item(State#state.history_raw, {T,now(),List});
				{false, true, false} -> %Insert to middle
					find_raw_place(State#state.history_raw, {T,now(),List})
					%lists:sort(fun({A,_,_},{B,_,_})-> B>A end,
					%			State#state.history_raw ++ [{T,now(),List}]
					%		   )
			end,
	case PrepData of
		ignore ->
			lager:info("Device ~p ignoring dup packet T ~p",[State#state.id, T]),
			State;
		{PHist,PRaw} ->
			%lager:debug("Device ~p Item ~p, prev ~p",[State#state.id, {T,now(),List}, PVal]),

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
						{PTime,_,Prv} when is_list(Prv) -> 
									   PPVal=case lists:keyfind(PTime,1,PrHist) of
												 false -> [];
												 {_,PVl} when is_list(PVl) -> PVl;
												 _Any -> 
													 lager:error("Badmatch ~p",[_Any]),
													 []
											 end,
									   {PPVal,Prv,T-PTime};
								   _Any -> 
							throw({badmatch,_Any,PRaw})
					end,

			SVals=process_variables(List, InCfg, PRawVal, DeltaT),

			SoftVars=case dict:find(lastpos,State#state.data) of % Software Odometer
						 {ok, [PLon,PLat]} ->
							 {Azimut,Distance}=gpstools:sphere_inverse({PLon,PLat},{Lon,Lat}),
							 [
							  {softodometer,Distance*1000},
							  {softcompass,Azimut}
							 ];
						 _ -> []
					 end,

			PrData=[{dt,T},
					{position,[Lon, Lat]},
					{dir, Dir},
					{delay, AT-T},
					{sp,Speed}] ++ SVals ++ SoftVars,
			%lager:info("vals ~p~n~p -> ~n~p",[DeltaT,PrevAval,PrData]),

			lager:debug("Car ~p src ~p",[State#state.id,List]),
			lager:debug("Car ~p agg ~p",[State#state.id,PrData]),

			NewState=case T > State#state.last_ptime of
						 true ->
							 UserPIlst=lists:usort([
													{ S#usersub.pi_name, S#usersub.params } || 
													S <- State#state.usersub,
													is_atom(S#usersub.pi_name) andalso S#usersub.pi_name =/= '' andalso S#usersub.pi_name =/= null
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
							 if T > State#state.last_ptime -> % only if it's newest one
									[{pi_display,State#state.sub_ev}];
								true -> []
							 end ++ 
							 case proplists:lookup(plugins,State#state.settings) of
								 none -> [];
								 {plugins,X} when is_list(X) -> 
									 lists:filter(fun(Xi) when is_atom(Xi) -> true;
													 (_) -> false
												  end, X);
								 _ -> []
							 end ++ 
							 maps:to_list(UserPI2h) ++ UserPI2l,
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
							 lager:info("Car ~p at ~p,~p (~p km/h) Pois ~p ~p delay ~p",Log),

							 lists:foreach(fun(En)->
												   lager:debug("Car ~p event emitters ~p",[State#state.id,En])
										   end,State#state.usersub),
							 EES=[ S || S <- State#state.usersub, is_atom(S#usersub.ee_name) andalso S#usersub.ee_name =/= '' ],
							 EEhSrc=case dict:find(eedata, State#state.data) of
										{ok, EEhVal} -> EEhVal;
										_ -> #{}
									end,
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
							   cur_poi=POIs, 
							   plugins_data=PState,
							   history_raw=PHist, % ++ [{T,now(),List}], 
							   history_processed=PrHist ++ [{T,PrData++EPrData}], 
							   chour=UnixHour,
							   data= dict:store(eedata, EEData,
												dict:store(lastpos, [Lon, Lat], State#state.data)
											   )
							  };
						 _ ->
							 State#state{
							   history_raw=PHist, % lists:sort(fun({A,_,_},{B,_,_})-> B>A end,PHist ++ [{T,now(),List}]),
							   history_processed= lists:sort(fun({A,_},{B,_})-> B>A end,PrHist ++ [{T,PrData}]), 
							   data= dict:store(lastpos, [Lon, Lat], dict:store(disorder, true, State#state.data)), 
							   chour=UnixHour}
					 end,
	dump_stat(NewState)
	end.


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
				{MSec,Sec,_}=now(),
				NextUnixHour=gpstools:floor((MSec*1000000+Sec)/3600),

				case UnixHour of
					CurH -> %current hour
						process_ds(List,State);
					NextH -> %next hour
						S1=switch_hour(State),
						process_ds(List,S1);
					_Any when _Any > NextUnixHour -> %data from future: incorrect time from device. ignore it.
						lager:error("Data from future!!! ~p: Current hour ~p, Data hour ~p",[State#state.id,State#state.chour, UnixHour]),
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
						process_ds(List,S1)
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
				  Res#state.data),
	D3=dict:store(last_ds,now(),D2),
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
											  {AName,processStruct(Args,Path,undefined, undefined)}
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
						 Lim=case proplists:get_value(limitsd,X,[null,null]) of
								[A,B] when 
									(is_integer(A) orelse is_float(A) orelse A==null) andalso
									(is_integer(B) orelse is_float(B) orelse B==null) ->
									 {delta,A,B};
								 _ ->
									 undefined
							 end,
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
								limits=Lim,
								variable=list_to_atom("v_"++binary_to_list(Var))
								%, ovfval=Ovf
							   };

					 {[<<"inputs">>],Num} when is_integer(Num) ->
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
	K=(B2-B1)/(A2-A1),
	[{A1,A2,B1,K}] ++ prepare_tartab([[A2,B2]|R]).



process_variables1(_List, _Pre, [], Acc, _Dt) ->
	Acc;

process_variables1(List, PreRaw, [X|Rest], Acc, Dt) ->
	case proplists:get_value(X#incfg.dsname,List) of
		undefined -> 
			process_variables1(List, PreRaw, Rest, Acc, Dt);
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
							_ when is_integer(Val0) ->
								(Val0 bsr Off) band ((1 bsl Bits)-1);
							_ -> 
								lager:notice("Invalid data for bin: ~p",[Val0]),
								0
						end;
					_ ->
						Val0
				end,
			
			VFVal=case Val of
					  null ->
						  null;
					  _ -> 
						  case X#incfg.factor of
							  1 -> Val;
							  undefined -> Val;
							  L when is_list(L) ->
								  %lager:debug("Input ~w tar ~p",[X#incfg.dsname,L]),
								  case tar(L,Val) of
									  overflow -> null;
									  underflow -> 0;
									  Xi when is_integer(Xi) -> Xi;
									  Xi when is_float(Xi) -> Xi
								  end;

							  Factor when is_integer(Factor) -> Val * Factor;
							  Factor when is_float(Factor) -> Val * Factor;
							  _Any -> 
								  lager:info("Factor ~p",[_Any]),
								  Val
						  end
				  end,

			NewAcc=
			case lists:keyfind(X#incfg.variable, 1, Acc) of
				false ->
					lists:keystore(X#incfg.variable, 1, Acc, 
								   { X#incfg.variable, VFVal });
				{_Name,PreVal} ->
					lists:keystore(X#incfg.variable, 1, Acc, 
								   { X#incfg.variable, PreVal+VFVal })
			end,
			%{bin,Offset,Len} -> 
			%lager:info("Binary ~p offset ~p len ~p",
			%		[X#incfg.dsname, Offset, Len]),
			%{0 ,{X#incfg.dsname,0,0}}
			%lager:debug("inCfg ~p Acc ~p",[X, NewAcc]),
			%
			process_variables1(List, PreRaw, Rest, NewAcc, Dt)
	end.


process_variables(List, InCfg, PreVar, Dt) -> % {SVals,SOfvs}.
	process_variables1(List, PreVar, InCfg, [], Dt).

process_counter(undefined, _V2, _Limit) ->
	0;
process_counter(V1, V2, Limit) ->
	case V2-V1 of
		X when X>=0 -> X;
		_ -> Limit-V1+V2
	end.

get_event_subs(ID) ->
	lager:error("parse subs ~p",[ID]),
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
	 aggregator_config
	].

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
	ok.


