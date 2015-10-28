-module(esub2).

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

-export([gen_rand/0, device_get_sub/2, device_get_sub/1, split/2]).

-record(state, {redispid, chan, table, timer}).

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
	eredis_sub:psubscribe(Pid, [Chan]),
	lager:info("Eredis up ~p subscribe ~p",[Pid,Chan]),
	Tables={
	  ets:new(esub,[bag,protected,named_table,{keypos,1}]),
	  ets:new(esub_data,[bag,protected,named_table,compressed,{keypos,1}])
	 },
	process_all(Tables,"esub:"),
	Timer=erlang:send_after(10000,self(),cleanup),
	{ok, #state{redispid=Pid,chan=Chan, table=Tables, timer=Timer}}.

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
handle_info({pmessage,_Tpl,_Chan,Payload,SrcPid}, State) ->
	process_sub(State#state.table,Payload),
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info({subscribed,_Chan,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	{noreply, State};

handle_info(cleanup, State) ->
	erlang:cancel_timer(State#state.timer),
	Now=time_compat:erlang_system_time(seconds),
	[ process_sub(State#state.table,X) || X<-ets:select(esub_data,[{{'$1','_','$2'},[{'<','$2',Now}],['$1']}])],
	Timer=erlang:send_after(10000,self(),cleanup),
	{noreply, State#state{timer=Timer}};

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

process_all({Table, TableI}, Prefix) -> 
	Now=time_compat:erlang_system_time(seconds),
	Fun=fun(Worker) -> 
				{ok, Array} = eredis:q(Worker, [ "keys", Prefix ++ "*" ]),
				GetItems = fun(X) ->
								   %eredis:q(Worker, [ "expire", X, "5" ]),
								   {ok, TTL} = eredis:q(Worker, [ "ttl", X ]),
								   ITTL=binary_to_integer(TTL),
								   case ITTL > 0 of
									   true -> 
										   {ok, A1} = eredis:q(Worker, [ "smembers", X ]),
										   {X,[ catch binary_to_integer(M) || M <- A1 ] ,ITTL+Now};
									   _ -> 
										   {X,[],TTL}
								   end
						   end,
				R=[ GetItems(X) || X<-Array ],
				R
		end,
	Arr=poolboy:transaction(redis, Fun),

	Fx=fun({Name,List,TTL}) ->
			   process_sub2({Table,TableI},Name,List,TTL)
		   end,
	Notify=lists:usort(lists:flatten(lists:map(Fx,Arr))),
	Fnotify=fun(E) ->
					case global:whereis_name({device,E}) of
						Pid when is_pid(Pid) ->
							gen_server:cast(Pid,{sub,all});
						undefined ->
							ok
					end
			end,
	lager:debug("Notify ~p",[Notify]),
	lists:foreach(Fnotify, Notify),
	%[ process_sub(X)  || X <- Arr ].
	%Time3=now(),
	%TD=transpose2(Arr,gb_trees:empty()),
	%lager:info("Transpose ~p",[timer:now_diff(now(),Time3)/1000]),
	%TD.
	ok.


process_sub2({Table, TableI}, Name, List, TTL) -> 
	{Add,Del} = case ets:lookup(TableI,Name) of
					[] ->
						{List,[]};
					[{_,XAr,_XTTL}] ->
						{lists:subtract(List,XAr),
						 lists:subtract(XAr,List)}
				end,
	lager:debug("Found ~p: add ~p del ~p",[Name,Add,Del]),
	FAdd=fun(Elm) ->
				 ets:insert(Table,{Elm,Name})
		 end,
	lists:foreach(FAdd,Add),
	FDel=fun(Elm) ->
				 ets:delete_object(Table,{Elm,Name})
		 end,
	lists:foreach(FDel,Del),
	case ets:member(TableI,Name) of
		true -> ets:delete(TableI,Name);
		_ -> ok
	end,
	case length(List) of
		0 -> ok;
		N when is_integer(N), N>0 ->
			ets:insert(TableI,{Name,List,TTL})
	end,
	%lager:info("N ~p S ~p T ~p",[Name,List,TTL]),
	lists:usort(Add++Del).

process_sub({Table, TableI}, Name) when is_binary(Name) -> 
	List=process_subr(Name, 0),
	TTL=poolboy:transaction(redis,
								  fun(Worker) -> 
										  {ok, STTL} = eredis:q(Worker, [ "TTL", Name ]),
										  list_to_integer(binary_to_list(STTL))
								  end),
	Now=time_compat:erlang_system_time(seconds),
	Notify=case TTL>0 of
		true -> 
			process_sub2({Table,TableI},Name,List,TTL+Now);
		_ -> 
			process_sub2({Table,TableI},Name,[],Now-1)
	end,
	Fnotify=fun(E) ->
					case global:whereis_name({device,E}) of
						Pid when is_pid(Pid) ->
							gen_server:cast(Pid,{sub,xsplit_type(Name)});
						undefined ->
							ok
					end
			end,
	lager:debug("Notify ~p",[Notify]),
	lists:foreach(Fnotify, Notify);


process_sub(T, Name) when is_list(Name) -> 
	process_sub(T, list_to_binary(Name)).

process_subr(Name, Cursor) ->
	lager:debug("Process_sub ~p, ~p",[Name, Cursor]),
	{N,Array}=poolboy:transaction(redis,
		fun(Worker) -> 
			{ok, [Next, Array]} = eredis:q(Worker, [ "sscan", Name, Cursor, "count", "10" ]),
			{Next, [ case catch binary_to_integer(M) of N when is_integer(N) -> N; _ -> 0 end || M <- Array ]}
		end
	),
	case N of
		<<"0">> ->
			Array;
		Any when is_binary(Any) ->
			%length(Array)+process_sub(Name,Any)
			Array ++ process_subr(Name,Any)
	end.

gen_rand() -> 
	Fx=fun(Worker) -> 
			X="esub:position:s." ++ md5:md5_hex(io_lib:format("~p", [random:uniform()])), 
			RndList=[ erlang:trunc(random:uniform()*500) || _X <- lists:seq(1,200) ],
			eredis:q(Worker, ["sadd", X ] ++ RndList ), 
			eredis:q(Worker, ["expire", X, 60]), 
			eredis:q(Worker, ["publish", "esub:position", X]) 
	end,
	poolboy:transaction(redis,Fx).

device_get_sub(Device) ->
	{ok, M}=device_get_sub(any, Device),
	Fx=fun({C,X},Dict) ->
			   dict:append_list(C,[X],Dict)
	   end,
	dict:to_list(lists:foldl(Fx,dict:new(),M)).

device_get_sub(SPattern, Device) ->
	Pattern=case SPattern of
				B when is_binary(B) -> B;
				B when is_list(B) -> list_to_binary(B);
				any -> any;
				B when is_atom(B) -> list_to_binary(atom_to_list(B));
				_ -> any
			end,
	Sub=ets:lookup(esub,Device),
	Fun=fun({_,X}) ->
				[_,Type,Channel]=split(X,":"),
				case Pattern of 
					any -> {true, {list_to_atom(binary_to_list(Type)),Channel}};
					Type -> {true, Channel};
					_ -> false
				end
		end,

	{ok,lists:filtermap(Fun,Sub)}.

xsplit_type(Ident) ->
	[_,Type,_]=split(Ident,":"),
	Type.

split(Binary, Chars) ->
	split(Binary, Chars, 0, 0, []).

split(Bin, Chars, Idx, LastSplit, Acc)
  when is_integer(Idx), is_integer(LastSplit) ->
	Len = (Idx - LastSplit),
	case Bin of
		<<_:LastSplit/binary,
		  This:Len/binary,
		  Char,
		  _/binary>> ->
			case lists:member(Char, Chars) of
				false ->
					split(Bin, Chars, Idx+1, LastSplit, Acc);
				true ->
					split(Bin, Chars, Idx+1, Idx+1, [This | Acc])
			end;
		<<_:LastSplit/binary,
		  This:Len/binary>> ->
			lists:reverse([This | Acc]);
		_ ->
			lists:reverse(Acc)
	end.

