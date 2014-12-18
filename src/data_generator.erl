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

-record(state, {rect,speedlim,position,target,cspeed,debug,waypoint,az,imei,timer,fuel,path}).

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

	Timeout0=case State0#state.debug of 
			 debug ->
				 2;
			 _ -> 3+round(random:uniform()*30)
		 end,
	State=case gpstools:dist(State0#state.position,State0#state.target)>0.1 of 
			  true ->
				  State0#state{az=gpstools:azimuth(State0#state.position,
												   State0#state.target)};
			  false -> %finished
				  case Variant of
					  first ->
						  Dest=case Fuel > 15 of
								   true ->
									   rand_point(State0#state.rect);
								   _ -> 
									   {36.1648,51.7176}
							   end,
						  State0#state{target=Dest, 
									   cspeed=rand_speed(State0#state.speedlim),
									   az=gpstools:azimuth(State0#state.position,Dest)};
					  second -> 
						  WP=case State0#state.waypoint of
								 N when is_integer(N) -> N;
								 _ -> 1
							 end,
						  {Dest,NewPath,NPoint} = case State0#state.path of
													  [] ->
														  NP=round(rand_speed([1,12])),
														  [I1 | I2] =  gpstools:get_path(WP,NP),
														  {I1, I2, NP};
													  L when is_list(L) ->
														  case L of
															  [] -> 
																  NP=round(rand_speed([1,12])),
																  [I1 | I2] =  gpstools:get_path(WP,NP),
																  {I1, I2, NP};
															  [I1 | I2] ->
																  {I1, I2, WP}
														  end;
													  _ ->
														  NP=round(rand_speed([1,12])),
														  [I1 | I2] =  gpstools:get_path(WP,NP),
														  {I1, I2, NP}
												  end,
						  %lager:info("New dst ~p ~p",[Dest,WP]),
						  State0#state{target=Dest, 
									   waypoint=NPoint,
									   cspeed=rand_speed(State0#state.speedlim),
									   az=gpstools:azimuth(State0#state.position,Dest),
									   path=NewPath
									  }
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
	Timer=case Refuel of
		false -> 
				  erlang:send_after(1000*Timeout,self(),{chpos, P2});
			  Rf -> 
				  erlang:send_after(10000,self(),{refuel, Fuel, Rf, 0})
		  end,

	St1=State#state{timer=Timer,fuel=Fuel,cspeed=Spd,az=AZ},
	senddata(St1),
	St1.


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
	Here={36.1648, 51.7176}, %rand_point(Rect),
       	%Dest=rand_point(Rect),
	{ok, step(#state{
		     imei=IMEI,
		     rect=Rect,
		     position=Here,
		     target=Here,
		     cspeed=10,
		     speedlim=Speed,
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
	S2=step(State#state{position=NewPos,fuel=NFuel}),
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
			senddata(Ns),
			Ns
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
	lager:info("debug ~p",[self()]),
	{noreply, State#state{debug=debug}}; 

handle_info(sliv, State) ->
	lager:info("sliv ~p",[self()]),
	{noreply, State#state{fuel=10}}; 

handle_info(nodebug, State) ->
	lager:info("debug ~p",[self()]),
	{noreply, State#state{debug=undefined}}; 

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
senddata(State) ->
	{Lon, Lat} = State#state.position,
	{Ms,S,_Us} = erlang:now(),
	Data={struct,[
		      {imei,list_to_binary(State#state.imei)},
		      {dir,State#state.az},
		      {sp,State#state.cspeed},
		      {dt,(Ms*1000000)+S},
		      {in0,State#state.fuel},
		      {position,{array,[Lon, Lat]}}
		     ]},
	Document=iolist_to_binary(mochijson2:encode(Data)),
	%lager:info("Send ~p",[Document]),

	Redis=fun(W) -> 
			      eredis:q(W, [ "publish", "source", Document])
	      end,
	poolboy:transaction(redis,Redis).


