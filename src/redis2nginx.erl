-module(redis2nginx).

-behaviour(gen_server).

%% API functions
-export([start_link/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {redispid,url,chan,strip}).

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
start_link(Host, Port, Chan, Url) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Host, Port, Chan, Url], []).

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
init([Host, Port, {Chan, Strip}, Url]) ->
	{ok, Pid} = eredis_sub:start_link(Host, Port, ""),
	lager:info("Eredis up ~p: ~p:~p",[Pid,Host,Port]),
	eredis_sub:controlling_process(Pid),
	eredis_sub:psubscribe(Pid, [Chan]),
	lager:info("Eredis up ~p subscribe ~p",[Pid,Chan]),
	Url2=case is_binary(Url) of
		     true -> Url;
		     _ -> list_to_binary(Url)
	     end,
	{ok, #state{redispid=Pid,url=Url2,chan=Chan,strip=Strip}}.

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
handle_cast({push,Chan,Payload}, State) ->
	Url=binary_to_list(binary:replace(Chan,State#state.strip,State#state.url,[])),
	{ok,{RC,_RH,Body}}=httpc:request(post, {Url, [], "text/json", escape_payload(Payload)}, [], []),
	lager:debug("Push ~p:~p ~p",[Url,RC,Body]),
	{noreply, State};

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
handle_info({pmessage,_PSub,Chan,Payload,SrcPid}, State) ->
	Url=binary_to_list(binary:replace(Chan,State#state.strip,State#state.url,[])),
	{ok,{RC,_RH,Body}}=httpc:request(post, {Url, [], "text/json", escape_payload(Payload)}, [], []),
	lager:debug("Push ~p:~p",[Url,RC,Body]),
	eredis_sub:ack_message(SrcPid),
	{noreply, State};


handle_info(Info, State) ->
	eredis_sub:ack_message(State#state.redispid),
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
%%%
escape_payload(Payload) ->
	binary:replace(Payload,<<"\"">>,<<"\\\"">>,[global]).
