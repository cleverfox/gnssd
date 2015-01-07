-module(data_generator).

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

-record(state, {rect,speedlim,position,target,cspeed,extra,waypoint,az,imei,timer,fuel,path}).

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
start_link(IMEI) ->
    gen_server:start_link(?MODULE, [IMEI], []).

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
rand_point({{X1,Y1},{X2,Y2}}) ->
	Px=lists:min([X1,X2])+(abs(X2-X1)*random:uniform()),
	Py=lists:min([Y1,Y2])+(abs(Y2-Y1)*random:uniform()),
	{Px,Py}.

rand_speed([Sp1,Sp2]) ->
	Sp1+(round(random:uniform()*(Sp2-Sp1)*10)/10).

step(State0) ->
	Variant = second,
	Fuel=State0#state.fuel,
%	Fuel=case State0#state.fuel of 
%			 M when is_float(M) -> 
%				 case (State0#state.imei == "10001") andalso (M > 90) andalso (random:uniform() > 0.95) of %car 10001 pizdit benzin
%					 true -> M - 30;
%					 false -> M
%				 end;
%			 _ -> 40.0
%		 end,

	Timeout0=3,
%	Timeout0=case State0#state.debug of 
%			 debug ->
%				 2;
%			 _ -> 3+round(random:uniform()*30)
%		 end,
	{State,Finished}=case gpstools:dist(State0#state.position,State0#state.target)>(State0#state.cspeed/(3600/Timeout0)) of 
			  true ->
							 {State0#state{az=gpstools:azimuth(State0#state.position,
												   State0#state.target)}, false};
			  false -> %finished
				  case Variant of
					  first ->
						  Dest=case Fuel > 15 of
								   true ->
									   rand_point(State0#state.rect);
								   _ -> 
									   {36.1648,51.7176}
							   end,
						  {State0#state{target=Dest, 
									   cspeed=rand_speed(State0#state.speedlim),
									   az=gpstools:azimuth(State0#state.position,Dest)},true};
					  second -> 
						  WP=case State0#state.waypoint of
								 N when is_integer(N) -> N;
								 _ -> 1
							 end,
						  {Dest,NewPath,NPoint,Fin} = case State0#state.path of
													  L when is_list(L) ->
														  case L of
															  [] -> 
																  %NP=round(rand_speed([1,12])),
																  {ok,_,[{NP}]}=psql:equery("select id from waypoints where active and id <> $1 order by random() limit 1",[WP]),
																  [I1 | I2] =  gpstools:get_path(WP,NP),
																  {I1, I2, NP, true};
															  [I1 | I2] ->
																  {I1, I2, WP, false}
														  end;
													  _ ->
														  {ok,_,[{NP}]}=psql:equery("select id from waypoints where active and id <> $1 order by random() limit 1",[WP]),
														  %NP=round(rand_speed([1,12])),
														  [I1 | I2] =  gpstools:get_path(WP,NP),
														  {I1, I2, NP, false}
												  end,
						  %lager:info("New dst ~p ~p",[Dest,WP]),
						  {State0#state{target=Dest, 
									   waypoint=NPoint,
									   cspeed=rand_speed(State0#state.speedlim),
									   az=gpstools:azimuth(State0#state.position,Dest),
									   path=NewPath
									  },Fin}
				  end
		  end,
	SDist=State#state.cspeed*(Timeout0/3600),
	{AZ,RDist}=gpstools:sphere_inverse(State#state.position,State#state.target),

	{Dist,Timeout,Spd}=case SDist>RDist of
%	true -> {RDist, round(RDist/State#state.cspeed)*3600, State#state.cspeed};
		true -> {RDist, Timeout0, SDist / (Timeout0/3600)};
		_ -> {SDist, Timeout0, State#state.cspeed}
	end,
	%lager:info("Dist ~p, Time ~p, Spd ~p (ospd ~p)",[Dist, Timeout, Spd, State#state.cspeed]),
	P2=gpstools:sphere_direct(State#state.position,AZ,Dist),


	GAS=gpstools:dist(State0#state.position,{36.1648,51.7176})<0.1,

	RefuelProp = case Fuel > 10 of %refuel propability
					 true -> 
						 case Fuel > 40 of
							 true -> 0;
							 false -> 0.0003
						 end;
					 false -> 
						 case GAS of 
							 true -> 1; %zapravka
							 false -> 0.01 %net zapravki
						 end
				 end,
	Refuel = case random:uniform() < RefuelProp of %if refuel - generate amount
				 true -> round(random:uniform()*6+1)*5;
				 false -> false
			 end,
	%lager:debug("Device ~p, finished ~p",[State0#state.imei,Finished]),
	Timer=case Refuel of
		false -> 
				  case Finished of 
					  true -> 
						  erlang:send_after(30000,self(),{wait, round(random:uniform()*10)+2, finished});
					  _ ->
						  case random:uniform() < 0.01 of % random stop
							  true -> 	  
								  erlang:send_after(5000,self(),{wait, round(random:uniform()*32), pause});
							  _ -> 
								  erlang:send_after(1000*Timeout,self(),{chpos, P2})
						  end
				  end;
			  Rf -> 
				  erlang:send_after(10000,self(),{refuel, Fuel, Rf, 0})
		  end,

	St1=State#state{timer=Timer,fuel=Fuel,cspeed=Spd,az=AZ},
	senddata(St1,false).


init([IMEI]) ->
	random:seed(erlang:now()),
	Speed=[30,100],
	%Rect={{36.091812,51.718362}, {36.233605,51.800388}}, 
	Rect={{35.992200,51.832806},{36.316297,51.645568}},
	%Here={36.190186,51.739678},% rand_point(Rect),
	%Dest={36.190186,51.741893}, %vverh
	%Dest={36.193948,51.739678}, %vpravo
	%Dest={36.190186,51.737823}, %vniz
	%Dest={36.186443,51.739678}, %vlevo
	{ok,_,[{NP,Lon,Lat}]}=psql:equery("select id,lon,lat from waypoints where active order by random() limit 1",[]),
	Here={Lon,Lat}, %rand_point(Rect),
       	%Dest=rand_point(Rect),
	{ok, step(#state{
		     imei=IMEI,
			 path=[],
			 waypoint=NP,
		     rect=Rect,
		     position=Here,
		     target=Here,
		     cspeed=10,
		     speedlim=Speed,
			 extra=dict:new(),
		     az=0 %gpstools:azimuth(Here,Dest)
		    })}.

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
handle_cast(_Msg, State) ->
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
handle_info({chpos,NewPos}, State) ->
	erlang:cancel_timer(State#state.timer),
	Dist=gpstools:dist(State#state.position,NewPos),
	LKM=12, %liters/100km
	Fuel=LKM*Dist/100,
	NFuel=case State#state.fuel of M when is_float(M) -> M; _ -> 10 end -Fuel,
	POdometer=case dict:find(odometer,State#state.extra) of 
				  {ok, TS} -> TS;
				  _ -> 0
			  end,
	{_,RDist}=gpstools:sphere_inverse(State#state.position,NewPos),
	Odometer=POdometer+RDist,
	S2=step(State#state{position=NewPos,fuel=NFuel,extra=dict:store(odometer,Odometer,State#state.extra)}),
	%lager:info("ChPos ~p",[NewPos]),
%	lager:info("NewPos ~p, Heading to ~p ~p, ~p",[
%						      NewPos,
%						      S2#state.target,
%						      S2#state.cspeed,
%						      gpstools:sphere_inverse(NewPos,S2#state.target)
%						     ]),
	{noreply, S2};

handle_info({refuel, OldFuel, NewFuel, Done}, State) ->
	lager:info("Car ~p refuel ~p of ~p done",[State#state.imei,Done,NewFuel]),
	erlang:cancel_timer(State#state.timer),
	S2=case Done >= NewFuel of
		true ->
			erlang:send_after(5000,self(),{chpos, State#state.position}),
			State#state{cspeed=rand_speed(State#state.speedlim)};
		false ->
			   NextF=Done+(5+random:uniform()*0.5),
			   NextF1=case NextF > NewFuel of
				   true -> NewFuel;
				   false -> NextF
			   end,
			erlang:send_after(10000,self(),{refuel, OldFuel, NewFuel, NextF1}),
			Ns=State#state{cspeed=0,fuel=OldFuel+Done},
			senddata(Ns,false)
	end,
	{noreply, S2};

handle_info({wait, Rest, Type}, State) ->
	handle_info({wait, Rest, Type, 0}, State);

handle_info({wait, Rest, Type, Num}, State) ->
	%lager:info("Car ~p waiting ~p/~p ~p",[State#state.imei,Rest,Num,Type]),
	erlang:cancel_timer(State#state.timer),
	W=case Type of
		  pause -> 5000;
		  _ -> 30000
	  end,
	S2=case Rest < 1 of
		true ->
			erlang:send_after(W,self(),{chpos, State#state.position}),
			State#state{cspeed=rand_speed(State#state.speedlim)};
		false ->
			erlang:send_after(W,self(),{wait, Rest-1, Type, Num+1}),
			Ns=State#state{cspeed=0},
			senddata(Ns,Num==0)
	end,
	{noreply, S2};

handle_info({gofromto, From, To }, State) ->
	lager:info("Go from ~p to ~p",[From, To]),
	{noreply, State#state{waypoint=To, path=gpstools:get_path(From,To)}}; 

handle_info({bound, Rect={{_,_},{_,_}} }, State) ->
	lager:info("Bound ~p: ~p",[self(), Rect]),
	{noreply, State#state{rect=Rect}}; 

handle_info(fly, State) ->
	lager:info("Fly ~p",[self()]),
	{noreply, State#state{cspeed=600, speedlim=[500,1000]}}; 

handle_info(nofly, State) ->
	lager:info("noFly ~p",[self()]),
	{noreply, State#state{cspeed=80, speedlim=[30,100]}}; 

handle_info(debug, State) ->
	D1=State#state.extra,
	lager:info("debug ~p",[self()]),
	{noreply, State#state{extra=D1}}; 

handle_info(sliv, State) ->
	lager:info("sliv ~p",[self()]),
	{noreply, State#state{fuel=10}}; 

handle_info(nodebug, State) ->
	D1=State#state.extra,
	lager:info("debug ~p",[self()]),
	{noreply, State#state{extra=D1}}; 

handle_info(stop, State) ->
	lager:info("Stop ~p",[self()]),
    {stop, normal, State};

handle_info(_Info, State) ->
	lager:info("INFO ~p",[_Info]),
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
%%%
senddata(State,Force) ->
	{Lon, Lat} = State#state.position,
	{Ms,S,_Us} = Now = erlang:now(),
	UT=(Ms*1000000)+S,
	SOk=case Force of 
			true -> 
				true;
			_ -> 
				case dict:find(nextsend,State#state.extra) of 
					{ok, TS} -> case timer:now_diff(Now, TS) > 0 of
									true -> true;
									_ -> case dict:find(sentaz,State#state.extra) of
											 {ok, AZ} -> abs(AZ-State#state.az) > 30 ;
											 _ -> false
										 end
								end;
					_ -> true
				end
		end,
	%lager:info("Now ~p, exp ~p, ok: ~p",[Now,dict:find(nextsend,State#state.extra), SOk]),
	case SOk of 
		true -> 
			Data={struct,[
						  {imei,list_to_binary(State#state.imei)},
						  {dir,State#state.az},
						  {sp,State#state.cspeed},
						  {dt,UT},
						  {in0,State#state.fuel},
						  {in2,0.5},
						  {in1,
						   case dict:find(odometer,State#state.extra) of 
							   {ok, TMP} -> TMP; _ -> 0
						   end
						  },
						  {position,{array,[Lon, Lat]}}
						 ]},
			Document=iolist_to_binary(mochijson2:encode(Data)),
			%lager:info("Send ~p",[Document]),

			Redis=fun(W) -> 
						  eredis:q(W, [ "publish", "source", Document])
				  end,
			poolboy:transaction(redis,Redis),
			UT2=UT+3+round(random:uniform()*57),
			Next={gpstools:floor(UT2/1000000),gpstools:mod(UT2,1000000),0},
			State#state{extra=dict:store(sentaz,State#state.az,dict:store(nextsend,Next,State#state.extra))};
		_ ->
			State
	end.

