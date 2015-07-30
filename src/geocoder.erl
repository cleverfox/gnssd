-module(geocoder).

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
handle_info({message,_Chan,_Payload,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	NewState=popmsg(State,10),
	{noreply, NewState};

handle_info({subscribed,_Chan,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info(pull, State) ->
	erlang:cancel_timer(State#state.timer),
	T1=now(),
	NewState=popmsg(State,1),
	lager:info("Performance ~p /sec",[1/(((timer:now_diff(now(),T1)/1000000)))]),

	{ok, Count} = poolboy:transaction(redis,fun(W)-> eredis:q(W,[ "llen", "geocode" ]) end),
	Timeout=case Count of 
				0 -> 
					5000;
				<<"0">> -> 
					5000;
				_ -> 
					50
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
										  eredis:q(W,[ "rpop", "geocode" ])
								  end),
	case POP of 
		{ok, Payload} when is_binary(Payload) ->
			State2=try mochijson2:decode(Payload) of
				{struct,List} when is_list(List) ->
						   case proplists:get_value(<<"coords">>,List) of
									  [Lon,Lat] -> 
										  Url="http://195.234.3.44:21000/nominatim/reverse?format=json&lat="++
										  float_to_list(Lat,[{decimals, 10},compact])++
										  "&lon="++
										  float_to_list(Lon,[{decimals, 10},compact])++
										  "&zoom=18&addressdetails=0",
										  try
												  {ok, {_,_,Body}} = httpc:request(get, {Url, []}, [], []),
												  {struct,JS}=mochijson2:decode(Body),
												  Name=proplists:get_value(<<"display_name">>,JS),

												  Dev=proplists:get_value(<<"device">>,List),
												  Hr=proplists:get_value(<<"hour">>,List),
												  Ev=proplists:get_value(<<"ev">>,List),
												  Key=proplists:get_value(<<"key">>,List),
												  KeyS={type,events, device,Dev, hour,Hr},
												  Data={ <<Key/binary,".",Ev/binary,"_txt">>, Name },
												  Res=mng:ins_update(mongo,<<"events">>, KeyS, Data),
												  %Res={KeyS,Data},
%												  lager:info("update ~p ~p ~p ~p -> ~p",[Dev, Hr, Key, Ev, Res]),

												  Name
											  catch _:_ -> error
											  end,
										  %lager:info("POP ~p ~p",[List, BJS]),
										  ok;
									  _ ->
										  error
								  end,
						   State;
				_Any -> 
					lager:error("Can't parse ~p",[Payload]),
					State
			catch
				error:Err ->
					lager:error("Can't parse ~p: ~p",[Err, Payload]),
					State
			end,
			popmsg(State2,Rest-1);
		{ok, undefined} ->
			State;
		_ ->
			State
	end.

