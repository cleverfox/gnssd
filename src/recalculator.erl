-module(recalculator).

-behaviour(gen_server).

%% API functions
-export([start_link/5,start_link/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, { 
		  car_id,
		  actions,
		  task,
		  userid,
		  total,
		  done
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
start_link(CarID,T1,T2,Actions) ->
    gen_server:start_link(?MODULE, [CarID,T1,T2,Actions, undefined], []).

start_link(CarID,T1,T2,Actions,UserID) ->
    gen_server:start_link(?MODULE, [CarID,T1,T2,Actions, UserID], []).

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
init([CarID,T1,T2,Actions,UserID]) ->
	H1=trunc(T1/3600),
	H2=trunc(T2/3600),
	lager:info("Recalc ~p, ~p-~p: ~p",[CarID,H1,H2,Actions]),
	gen_server:cast(self(), run_task),
	{ok, #state{
			car_id=CarID,
			actions=Actions,
			task=lists:seq(H1,H2),
			total=(H2-H1)+1,
			done=0,
			userid=UserID
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
handle_cast(run_task, #state{task=[]} = State) ->
	gen_server:cast(recalculator_dispatcher, {finished, State#state.car_id}),
	{stop, normal, State};

handle_cast(run_task, #state{task=[CurT|RestT]} = State) ->
%	erlang:send_after(1000,self(),{finish}),
%	{noreply, State};
	Res=device:init([State#state.car_id, CurT,sync]),
	Done=State#state.done+1,
	lager:info("recalc(~p,~p)=~p, to do ~p (~p of ~p)",
			   [State#state.car_id, CurT, Res, length(RestT),
			   Done,State#state.total]),
	if is_binary(State#state.userid) ->
		   JSData=jsx:encode(#{
					type=> <<"aggregate_progress">>,
					device_id => State#state.car_id,
					total => State#state.total,
					done => Done
				   }
							),
		   lager:info("recalc ~p",[{push,<<"push:",(State#state.userid)/binary>>,JSData}]),

		   gen_server:cast(redis_set,{mcmd, [
											 [ "hset", <<"recalc:",(State#state.userid)/binary>>, 
											   integer_to_binary(State#state.car_id), 
											   <<(integer_to_binary(Done))/binary,":",
												 (integer_to_binary(State#state.total))/binary>>
											 ],
											 [ "expire", <<"recalc:",(State#state.userid)/binary>>, 7200 ]
											] }),	
		   gen_server:cast(redis2nginx,{push,<<"push:",(State#state.userid)/binary>>,JSData}),
		   gen_server:cast(recalculator_dispatcher, {progress,
													 State#state.car_id,
													 State#state.userid,
													 State#state.total,
													 Done
													});
	   true ->
		   ok
	end,
	gen_server:cast(self(), run_task),
	{noreply, State#state{task=RestT,done=Done}};
	
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
handle_info({finish},State) ->
	gen_server:cast(recalculator_dispatcher, {finished, State#state.car_id}),
	{stop, normal, State};

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
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


