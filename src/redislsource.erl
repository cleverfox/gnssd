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
		  timer
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
			case timer:now_diff(now(),Time)<600000000 of
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
					D2=dict:store(IMEI,{Data,now()},Dict),
					{ok, Data, D2};
				_ -> {error, none, Dict}
			end
	end.

handle_info({message,_Chan,_Payload,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	NewState=popmsg(State,10),
	{noreply, NewState};

handle_info({subscribed,_Chan,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info(pull, State) ->
	erlang:cancel_timer(State#state.timer),
	NewState=popmsg(State,10000),

	{ok, Count} = poolboy:transaction(redis,fun(W)-> eredis:q(W,[ "llen", "source" ]) end),
	Timeout=case Count of 
				0 -> 
					5000;
				<<"0">> -> 
					5000;
				_ -> 
					100
	end,
	{noreply,
	 NewState#state{
	   timer = erlang:send_after(Timeout, self(), pull)
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

popmsg(State, 0) ->
	State;

popmsg(State, Rest) ->
	POP=poolboy:transaction(redis,fun(W)->
										  eredis:q(W,[ "rpop", "source" ])
								  end),
	case POP of 
		{ok, Payload} when is_binary(Payload) ->
			State2=try mochijson2:decode(Payload) of
				{struct,List} when is_list(List) ->
					case proplists:lookup(<<"imei">>,List) of
								{<<"imei">>, IMEIb} ->
									%lager:info("Ok, imei is ~p",[IMEIb]),
									case imei2deviceID(binary_to_list(IMEIb),State#state.imeicache) of
										{ok, ID, Dict2} -> 
											flogger:log("log/source/"++binary_to_list(IMEIb)++".log", Payload),
											case global:whereis_name({device,ID}) of
												undefined -> 
													case supervisor:start_child(dev_sup,[ID]) of
														{ok,Pid} -> 
															flogger:log("log/source/dev_"++integer_to_list(ID)++".log", [start,Pid,Payload]),
															gen_server:cast(Pid,{ds, List});
														Any -> 
															lager:error("Can't start device: ~p",[Any]),
															flogger:log("log/source/dev_"++integer_to_list(ID)++".log", [cantstart,Payload])
													end;
												Pid ->
													flogger:log("log/source/dev_"++integer_to_list(ID)++".log", [exists,Pid,Payload]),
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
									lager:error("There is no imei in data packet :("),
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
			popmsg(State2,Rest-1);
		{ok, undefined} ->
			State;
		_ ->
			flogger:log("log/source/badpop.log", POP),
			lager:error("Bad source ~p",[POP]),
			State
	end.

