-module(redislsource).

-behaviour(gen_server).

%% API functions
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
		  redispid,
		  imeicache,
		  timer,
		  speed=[]
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
start_link(Host, Port) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Host, Port], []).

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
init([Host, Port]) ->
	{ok, Pid} = eredis_sub:start_link(Host, Port, ""),
	lager:info("Eredis up ~p: ~p:~p",[Pid,Host,Port]),
	eredis_sub:controlling_process(Pid),
	eredis_sub:subscribe(Pid, [<<"source_notify">>]),
	lager:info("Eredis up ~p",[Pid]),
	{ok, #state{
			redispid=Pid,
			imeicache=dict:new(),
			timer = erlang:send_after(10000, self(), pull)
		   }
	}.

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
imei2deviceID(IMEI,Dict) ->
	T=case dict:find(IMEI, Dict) of
		{ok, {ID, Time}} ->
			case (time_compat:erlang_system_time(seconds)-Time)<600 of
				true -> 
					{ok, ID};
				false ->
					error
			end;
		_ ->
			error
	end,
	case T of 
		{ok, MID} ->
			{ok, MID, Dict};
		error ->
			case psql:equery("select id from devices where imei=$1",[IMEI]) of
				{ok,_Hdr,[{Data}]} ->
					D2=dict:store(IMEI,{Data,time_compat:erlang_system_time(seconds)},Dict),
					{ok, Data, D2};
				_ -> {error, none, Dict}
			end
	end.

handle_info({message,_Chan,_Payload,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info({subscribed,_Chan,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info(wait, State) ->
	erlang:cancel_timer(State#state.timer),
	lager:error("I will wait 30 sec"),
	{noreply,
	 State#state{
	   timer = erlang:send_after(30000, self(), pull)
	  }
	};

handle_info({wait, N}, State) ->
	erlang:cancel_timer(State#state.timer),
	lager:error("I will wait ~p sec",[N]),
	{noreply,
	 State#state{
	   timer = erlang:send_after(N*1000, self(), pull)
	  }
	};

handle_info(pull, State) ->
	erlang:cancel_timer(State#state.timer),
	PopLimit=1000,
	T1=time_compat:erlang_system_time(micro_seconds),
	{NewState,PopRest}=poolboy:transaction(redis,fun(W)-> popmsg(W, State,PopLimit) end),
	T2=time_compat:erlang_system_time(micro_seconds),
	{ok, Count} = poolboy:transaction(redis,fun(W)-> eredis:q(W,[ "llen", "source" ]) end),
	Speed=try 
			  (PopLimit-PopRest)/((T2-T1)/1000000)
		  catch SEc:SEe ->
					lager:error("Can't calc speed ~p:~p PC ~p, T1 ~p, T2 ~p",
								[SEc,SEe, PopLimit-PopRest, T1, T2 ])
		  end,
	NSpd=if is_list(State#state.speed) ->
				NL=[Speed|State#state.speed],
				try
					{NSpd1,_}=lists:split(10,NL),
					NSpd1
				catch _:_ ->
						  NL
				end;
			true ->
				[Speed]
		 end,
	{SumSp,CntSp}=lists:foldl(fun(E,{Su,Cn}) ->
									  {Su+E,Cn+1}
							  end, {0,0}, NSpd),
	if PopRest < PopLimit ->
		   ICount=binary_to_integer(Count),
		   lager:info("Reqs: ~p. ~p/sec (~p/sec avg). In queue ~p (~p min)",[PopLimit-PopRest,Speed,SumSp/CntSp,ICount,trunc(ICount/Speed/60)]);
	   true ->
		   ok 
	end,

	Timeout=case Count of 
				0 -> 
					5000;
				<<"0">> -> 
					5000;
				_ -> 
					10
	end,
	{noreply,
	 NewState#state{
	   timer = erlang:send_after(Timeout, self(), pull),
	   speed = NSpd
	   }
	};

handle_info(Info, State) ->
	lager:info("Info ~p",[Info]),
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

popmsg(_T, State, 0) ->
	{State,0};

popmsg(W, State, Rest) ->
	POP=eredis:q(W,[ "rpop", "source" ]),
	case POP of 
		{ok, Payload} when is_binary(Payload) ->
			State2=try mochijson2:decode(Payload) of
					   {struct,List} when is_list(List) ->
						   case proplists:lookup(<<"imei">>,List) of
							   {<<"imei">>, IMEIb} ->
								   %lager:info("Ok, imei is ~p",[IMEIb]),
									case imei2deviceID(binary_to_list(IMEIb),State#state.imeicache) of
										{ok, ID, Dict2} -> 
											%flogger:log("log/source/"++binary_to_list(IMEIb)++".log", Payload),
											case global:whereis_name({device,ID}) of
												undefined -> 
													case supervisor:start_child(dev_sup,[ID]) of
														{ok,Pid} -> 
															%flogger:log("log/source/dev_"++integer_to_list(ID)++".log", [start,Pid,Payload]),
															gen_server:cast(Pid,{ds, List});
														Any -> 
															lager:error("Can't start device: ~p",[Any]),
															flogger:log("log/source/dev_"++integer_to_list(ID)++".log", [cantstart,Payload])
													end;
												Pid ->
													%flogger:log("log/source/dev_"++integer_to_list(ID)++".log", [exists,Pid,Payload]),
													gen_server:cast(Pid,{ds, List})
											end,
											State#state{imeicache=Dict2};
										_ -> 
											flogger:log("log/source/badimei.log", Payload),
											lager:error("There is unknown imei ~p in data packet",[IMEIb]),
											State
									end;
								_none ->
									flogger:log("log/source/noimei.log", Payload),
									lager:info("There is no imei in data packet :("),
									State
							end;
				_Any -> 
					flogger:log("log/source/badstruct.log", Payload),
					lager:error("Can't parse source ~p",[Payload]),
					State
			catch
				error:Err ->
					flogger:log("log/source/badjson.log", Payload),
					lager:error("Can't parse source ~p: ~p",[Err, Payload]),
					State
			end,
			popmsg(W,State2,Rest-1);
		{ok, undefined} ->
			{State,Rest};
		_ ->
			flogger:log("log/source/badpop.log", POP),
			lager:error("Bad source ~p",[POP]),
			{State,Rest}
	end.

