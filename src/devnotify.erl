-module(devnotify).

-behaviour(gen_server).

%% API functions
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {redispid, chan}).

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
start_link(Host, Port, Chan) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Host, Port, Chan], []).

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
init([Host, Port, Chan]) ->
	{ok, Pid} = eredis_sub:start_link(Host, Port, ""),
	lager:info("Eredis up ~p: ~p:~p",[Pid,Host,Port]),
	eredis_sub:controlling_process(Pid),
	eredis_sub:subscribe(Pid, [Chan]),
	{ok, #state{redispid=Pid,chan=Chan}}.

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
handle_info({message,Chan,Payload,SrcPid}, State) ->
	lager:info("Got message ~p",[Payload]),
	if Chan == State#state.chan ->
		   case Payload of 
			   <<"config:",DevID/binary>>  ->
				   case bin2device(DevID) of
					   undefined ->
						   ok;
					   M ->
						   lager:info("Casting ~p",[M]),
						   gen_server:cast(M, reload_settings),
						   ok
				   end;
			   <<"event_sub:",DevsIDs/binary>>  ->
				   lists:foreach(fun(DevID) ->
										 case bin2device(DevID) of
											 undefined ->
												 ok;
											 M ->
												 lager:info("Casting ~p",[M]),
												 gen_server:cast(M, reload_subs),
												 ok
										 end
								 end,
								 binary:split(DevsIDs,[<<",">>],[global])
								), 
				   ok;
			   _ ->
				   ok
		   end;
	   true ->
		   ok
	end,
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info({subscribed,_Chan,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info(Info, State) ->
	lager:info("Info ~p",[Info]),
	{noreply, State}.

bin2device(Bin) ->
	try 
		I=binary_to_integer(Bin),
		list_to_existing_atom("device_"++integer_to_list(I))
	catch _:_ ->
			  undefined
	end.

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


