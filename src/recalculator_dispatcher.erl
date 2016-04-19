-module(recalculator_dispatcher).

-behaviour(gen_server).

%% API functions
-export([start_link/3, recalc/1, recalc/2, uniq_list/2, append_tasks/4, test_ul/0, poptasks/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {redispid,
				chan,
				max_worker=50,
				timer,
				reclist
			   }).

%%%===================================================================
%%% API functions
%%%===================================================================
recalc(fuel) ->
	{ok,_,CarsB}=psql:squery("select id from devices where tagged"),
	[ recalc(fuel,binary_to_integer(X)) || {X}<-CarsB ].

recalc(fuel, Car) ->
	device:init([Car,0,synccfg]),
	C=mng:find(mongo,<<"devicedata">>,{device,Car,hour,{'$gte',trunc(1433106000/3600)}},{'_id',1}),
	Res=mc_cursor:rest(C),
	mc_cursor:close(C),
	io:format("Starting fuel recalc for car ~p (~p hours) ~n",[Car, length(Res)]),
	AddFun=fun(W)->
				   [
				   eredis:q(W,[
							   "lpush", 
							   <<"aggregate:express">>, 
							   <<(list_to_binary(mng:id2hex(ID)))/binary,":agg_fuelgauge">>
							  ])
				   || {'_id',ID} <-Res ],
				   eredis:q(W,[ "publish", <<"aggregate">>, <<"x">>])
		   end,
	poolboy:transaction(redis, AddFun),
	length(Res). 


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
	lager:info("Eredis up ~p subscribe ~p",[Pid,Chan]),
	{ok, #state{
			redispid=Pid,
			chan=Chan,
			timer=erlang:send_after(10000,self(),run_queue)
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
handle_call({max_workers, N}, _From, State) ->
	{reply, State#state.max_worker, State#state{max_worker=N}};

handle_call(status, _From, State) ->
	try
		Devlist=maps:keys(State#state.reclist),
		Res=lists:foldl(fun(Dev,{Devs,Hours}) ->
								{Devs+1,length(maps:get(Dev,State#state.reclist))+Hours}
					end,{0,0},Devlist),
		{reply, Res, State}
	catch Ec:Ee ->
			  {reply, {error, Ec,Ee}, State}
	end;

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
handle_cast({progress, Car_id, Userid, Total, Done }, State) ->
	lager:info("Progress ~p",[{Car_id, Userid, Total, Done }]),
	{noreply, State};
	
handle_cast({finished, _}, State) ->
	handle_cast(run_queue, State);
	%{noreply, State};

handle_cast(run_queue, State) ->
	case State#state.timer of 
		undefined -> ok;
		_ -> erlang:cancel_timer(State#state.timer)
	end,
	case poolboy:transaction(redis, fun(Worker) -> 
											eredis:q(Worker, [ "rpop", "recalc_cancel" ])
									end) of 
		{ok, TaskID } when is_binary(TaskID) -> 
			[ gen_server:cast(PID,{cancel,TaskID}) || {_,PID,_,_} <- supervisor:which_children(recalculator_sup) ],
			ok;
		_ -> 
			ok
	end,
	NewTasks = poptasks(State#state.reclist),

	AllowRun=case proplists:get_value(workers,supervisor:count_children(recalculator_sup)) of
				 undefined -> 
					 lager:error("Can't get worker count"),
					 false;
				 M when is_integer(M) ->
					 %lager:info("Workers ~p",[M]),
					 State#state.max_worker > M
			 end,
	lager:debug("Allow run ~p",[AllowRun]),
	S2=case AllowRun of
		   false -> 
			   State#state{timer=erlang:send_after(2000,self(),run_queue),reclist=NewTasks};
		   true  -> 
			   {L,Tasks}=case get_ready(NewTasks, time_compat:os_system_time(seconds)) of
					 {none, NewTasks2} ->
						 {false, NewTasks2};
					 {{Dev, Hours, Users},NewTasks2} -> 
							 lager:info("OK, ~p ~p ~p",[Dev,Hours,Users]),
							 case supervisor:start_child(recalculator_sup,[Dev,Hours,default,Users]) of
								 {ok, Pid} -> lager:info("Data recalculator ~p runned ~p",[Dev, Pid]),
											  true;
								 {error, Err} -> lager:error("Can't run data recalculator : ~p",[Err]),
												 error
							 end,
							 {true, NewTasks2}
				 end,
%			   lager:info("T1 ~p T2 ~p L ~p",[NewTasks,Tasks,L]),
			   %L: false - no more tasks, true - ok, error 
			   case L of 
				   true -> 
					   gen_server:cast(self(),run_queue),
					   State#state{reclist=Tasks};
				   false -> 
					   State#state{timer=erlang:send_after(10000,self(),run_queue),reclist=Tasks};
				   error -> 
					   lager:error("Error ~p",[L]),
					   State#state{timer=erlang:send_after(30000,self(),run_queue),reclist=Tasks};
				   {error,_} -> 
					   lager:error("Error ~p",[L]),
					   State#state{timer=erlang:send_after(30000,self(),run_queue),reclist=Tasks}
			   end
	   end,
	{noreply, S2};

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
handle_info(run_queue, State) ->
	gen_server:cast(self(),run_queue),
	{noreply, State};

handle_info({message,_Chan,_Payload,SrcPid}, State) ->
	%lager:info("Message ~p",[Payload]),
	eredis_sub:ack_message(SrcPid),
	gen_server:cast(self(),run_queue),
	{noreply, State};

handle_info({subscribed,_Chan,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

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

poptasks(State) ->
	case poolboy:transaction(redis, 
							 fun(Worker) -> 
									 eredis:q(Worker, [ "rpop", "recalc" ])
							 end) of 
		{ok,undefined} -> 
			State;
		{ok, RecvdData } -> 
			try
				[BDev,BStart,BEnd,Aggs|Rest]=binary:split(RecvdData,[<<":">>],[global]),
				UserID=case Rest of 
						   [XUserID|_] ->
							   XUserID;
						   _ -> undefined
					   end,
				Dev=binary_to_integer(BDev),
				Start=trunc(binary_to_integer(BStart)/3600),
				End=trunc(binary_to_integer(BEnd)/3600),
				lager:info("OK, ~p ~p ~p ~p",[Dev,Start,End,Aggs]),
				poptasks(uniq_list([{Dev, Start, End, UserID}], State))
			catch _:_ ->
					  lager:error("Can't parse recalc string, ~p",[RecvdData]),
					  State
			end
	end.


%[BDev,BStart,BEnd,Aggs|Rest]=binary:split(RecvdData,[<<":">>],[global]),
%Dev:Start:End:agg_list[:userid]
uniq_list([],State) ->
	State;

uniq_list([{Device,H1,H2,UserID}|Rest],State0) ->
%	T=time_compat:os_system_time(seconds),
	State=if is_map(State0) -> State0;
			 true -> #{}
		  end,
	DevTasks=maps:get(Device,State,[]),
	Tasks=append_tasks(H1,H2,UserID,DevTasks),
	
	uniq_list(Rest,maps:put(Device,Tasks,State)).

append_tasks(Hr1,Hr2,UserID,Tasks) ->
	T=time_compat:os_system_time(seconds)+60,
	case split_tasks(Hr1, Tasks, []) of
		{PrevTasks, []} -> %empty tasks or all tasks older
			PrevTasks ++ [{H, T, UserID} || H<-lists:seq(Hr1, Hr2)];
		{PrevTasks, [{PH2,_UID}=Post1|Post]} 
		  when Hr2 < PH2 -> %something exists but we have place
		   PrevTasks ++ [{H, T, UserID} || H<-lists:seq(Hr1, Hr2)] ++ [Post1|Post];
		{PrevTasks, TL } -> %need merge
			{Middle,Post} = split_tasks(Hr2, TL, []),
			PrevTasks ++ merge([{H, T, UserID} || H<-lists:seq(Hr1, Hr2)],Middle) ++ Post
	end.


merge(List1,List2) ->
	{Merged,MaxT}=lists:foldl(fun({H, T, Owner},{Acc,MT}) ->
						{
						 if(is_list(Owner)) ->
							   dict:append_list(H,Owner,Acc);
						   true ->
							   dict:append(H,Owner,Acc)
						 end,
						 if T>MT -> T;
							true -> MT
						 end
						}
				end, {dict:new(),0}, List1 ++ List2),
	lists:keysort(1,
				  lists:map(fun({Key,[Val]}) ->
									{Key, MaxT, Val};
							   ({Key,Val}) ->
									{Key, MaxT, Val}
							end,
							dict:to_list(Merged)
						   )
				 ).

split_tasks(_Hr,[],PL) ->
		  {lists:reverse(PL),[]};

split_tasks(Hr,[{H1,_,_}=Cur|Rest],PL) ->
	if(H1<Hr) ->
		  split_tasks(Hr, Rest, [Cur|PL]); %left task
	  true -> 
		  {lists:reverse(PL),[Cur|Rest]} %right task
	end.


check_dready([],State,_T) ->
	{none,State};

check_dready([Cur|Rest],State,T) ->
	DM=maps:get(Cur,State),
	case DM of 
		[] ->
			check_dready(Rest,maps:remove(Cur,State),T);
		_ ->
			case check_eready(DM, [], T) of
				{none, _} ->
					check_dready(Rest,State,T);
				{{Hours, Users}, []} ->
					{
					 {Cur, Hours, Users}, 
					 maps:remove(Cur, State)
				};
				{{Hours, Users}, NonReady} ->
					{
					 {Cur, Hours, Users}, 
					 maps:put(Cur, NonReady, State)
					}
			end
	end.

get_ready(State0, T) ->
	State=if is_map(State0) -> State0;
			 true -> #{}
		  end,
	check_dready(maps:keys(State),State,T).

aggregate(List) ->
	{Hours,_,Users}=lists:unzip3(List),
	{lists:usort(Hours),lists:usort(lists:flatten(Users))}.

check_eready([],[],_T) ->
	{none, []};

check_eready([],ReadyList,_T) ->
	{aggregate(ReadyList), []};

check_eready([{_,T1,_}=Cur|Rest],ReadyList,T) ->
	if(T1<T) ->
		  check_eready(Rest,[Cur|ReadyList],T);
	  ReadyList == [] ->
		  {none, [Cur|Rest]};
	  true ->
		  {aggregate(ReadyList), [Cur|Rest]}
	end.

test_ul() ->
	S0=uniq_list([{5,15,20,pavlik}],new),
	timer:sleep(200),
	S1=uniq_list([{5,28,31,ivan}],S0),
	timer:sleep(200),
	S2=uniq_list([{5,20,30,vovan}],S1),
	S3=uniq_list([{5,18,25,ivan}],S2),

	T=time_compat:os_system_time(seconds)+59+3,

	{T, get_ready(S3, T) }.


