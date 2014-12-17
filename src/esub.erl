-module(esub).

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

-export([process_sub/1,process_all/1,gen_rand/0, device_get_sub/2, split/2]).

-record(state, {redispid,chan}).

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
handle_info({pmessage,_Tpl,_Chan,Payload,SrcPid}, State) ->
	eredis_sub:ack_message(SrcPid),
	R=process_sub(Payload),
	lager:info("Event ~p",[R]),
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

transpose([],Dict) -> 
	Dict;

transpose([{ID, List, TTL} | Rest],Dict) -> 
	F=fun(I,TDict) ->
			  TTL2=case catch dict:fetch({ttl,I}, TDict) of 
					   T1 when is_integer(T1) ->
						   case TTL<T1 of
							   true -> TTL;
							   false -> T1
						   end;
					   _ -> TTL
				   end,
			  D2=dict:append_list({l,I},[ID],TDict),
			  dict:store({ttl,I},TTL2,D2)
	  end,
	Dict2=lists:foldl(F,Dict,List),
	transpose(Rest,Dict2).


process_all(Prefix) -> 
	Arr=poolboy:transaction(redis,
							fun(Worker) -> 
									Time1=now(),
									{ok, Array} = eredis:q(Worker, [ "keys", Prefix ++ "*" ]),
									lager:info("Fetch keys ~p",[timer:now_diff(now(),Time1)/1000]),
									GetItems = fun(X) ->
													   %																			 eredis:q(Worker, [ "expire", X, "5" ]),
													   {ok, TTL} = eredis:q(Worker, [ "ttl", X ]),
													   case binary_to_integer(TTL) > 0 of
														   true -> 
															   {ok, A1} = eredis:q(Worker, [ "smembers", X ]),
															   {X,[catch binary_to_integer(M) || M <- A1 ] ,binary_to_integer(TTL)};
														   _ -> 
															   {X,[],TTL}
													   end
											   end,
									Time2=now(),
									R=[ GetItems(X) || X<-Array ],
									lager:info("Fetch values ~p",[timer:now_diff(now(),Time2)/1000]),
									R
							end
						   ),
	%[ process_sub(X)  || X <- Arr ].
	Time3=now(),
	TD=transpose(Arr,dict:new()),
	lager:info("Transpose ~p",[timer:now_diff(now(),Time3)/1000]),
	[ {N,dict:fetch({l,N},TD),dict:fetch({ttl,N},TD)} || {l,N} <- dict:fetch_keys(TD) ].

process_sub(Name) when is_binary(Name) -> 
	%lager:info("Row: ~p",[Name]), 
	R0=process_unsub(Name, 0),
	R=process_sub(Name, 0),
	{ok,Name,R0,R};

process_sub(Name) when is_list(Name) -> 
	process_sub(list_to_binary(Name)).

process_unsub(Name, Cursor) ->
	{N,Array}=poolboy:transaction(redis,
		fun(Worker) -> 
				{ok, [Next, Array]} = eredis:q(Worker, [ "sscan", <<"-",Name/binary>>, Cursor, "count", "10" ]),
				{Next, Array}
		end
	),
	lager:info("uns ~p",[Array]),
	Fw=fun(Worker) -> 
			   Fx=fun(E) ->
					      Key= <<"worker:",E/binary,":",Name/binary>>,
						  lager:info("Delete ~p",[Key]),
					      {ok, Pv} = eredis:q(Worker,["del",Key]),
					      case Pv of 
						      <<"1">> -> false;
						      _ -> true
					      end
			      end,
			   lists:filter(Fx, Array)
	   end,
	case length(Array) of
		0 ->
			ok;
		_ ->
			poolboy:transaction(redis,
					    fun(Worker) -> 
							    eredis:q(Worker, [ "del", <<"-",Name/binary>> ]),
							    ok
					    end
					   )
	end,
	Changes=poolboy:transaction(redis1,Fw),


	case N of
		<<"0">> ->
			length(Changes);
		Any when is_binary(Any) ->
			length(Changes)+process_unsub(Name,Any)
	end.

process_sub(Name, Cursor) ->
	{N,Array,TTL}=poolboy:transaction(redis,
		fun(Worker) -> 
			{ok, [Next, Array]} = eredis:q(Worker, [ "sscan", Name, Cursor, "count", "10" ]),
			{ok, STTL} = eredis:q(Worker, [ "TTL", Name ]),
			TTL=list_to_integer(binary_to_list(STTL)),
			{Next, Array, TTL}
		end
	),
	case TTL > 0
	of
		true ->
			Fw=fun(Worker) -> 
					   Fx=fun(E) ->
							      Key= <<"worker:",E/binary,":",Name/binary>>,
							      {ok, Pv} = eredis:q(Worker,["getset",Key,1]),
							      eredis:q(Worker,["expire",Key,TTL]),
							      case Pv of 
								      <<"1">> -> false;
								      _ -> true
							      end
					      end,
					   lists:filter(Fx, Array)
			   end,
			Changes=poolboy:transaction(redis1,Fw),
			lager:info("Name ~p, Changes: ~p",[Name,Changes]),
			Fnotify=fun(E) ->
						case global:whereis_name({device,list_to_integer(binary_to_list(E))}) of
							Pid when is_pid(Pid) ->
								gen_server:cast(Pid,{sub,xsplit_type(Name)});
							undefined ->
								ok
						end
				end,
			lists:foreach(Fnotify, Changes);


		_ ->
			lager:info("Can't use invalid and eternal subscribtions, ttl ~p",[TTL])
	end,

	%lager:info("Iter ~p, arr ~p",[N,LArr]),

	case N of
		<<"0">> ->
			length(Array);
		Any when is_binary(Any) ->
			length(Array)+process_sub(Name,Any)
	end.


gen_rand() -> 
	Fx=fun(Worker) -> 
			X="esub:position:s." ++ md5:md5_hex(io_lib:format("~p", [random:uniform()])), 
			RndList=[ erlang:trunc(random:uniform()*2000) || _X <- lists:seq(1,1000) ],
			eredis:q(Worker, ["sadd", X ] ++ RndList ), 
			eredis:q(Worker, ["expire", X, 1800]), 
			eredis:q(Worker, ["publish", "esub:position", X]) 
	end,
	poolboy:transaction(redis,Fx).

device_get_subi(BMatch, Cursor) when is_binary(Cursor) -> %this guard never allow
	{N, Array} = poolboy:transaction(redis1,
		fun(Worker) -> 
			{ok, [Nxt, Arr]} = eredis:q(Worker, [ "scan", Cursor, "match", BMatch  ]),
			lager:info("geti ~p ~p ~p",[Nxt, BMatch, length(Arr)]),
			{Nxt, [ xsplit(X) || X <- Arr ]}
		end),
	case N of
		<<"0">> ->
			Array;
		Any when is_binary(Any) ->
			Array ++ device_get_subi(BMatch, Any)
	end;

device_get_subi(BMatch, _Cursor) ->
	poolboy:transaction(redis1,
		fun(Worker) -> 
			{ok, Arr} = eredis:q(Worker, [ "keys", BMatch  ]),
			Arr % [ xsplit(X) || X <- Arr ]
		end).

device_get_mttl(_W,[],M) when is_integer(M), M>0, M<86400 -> M;
device_get_mttl(_W,[],_M) -> 86400;
device_get_mttl(W,[X | Rest ],M) ->
	{ok, TTLb} = eredis:q(W, [ "ttl", X]),
	TTL=list_to_integer(binary_to_list(TTLb)),
	case TTL>0 andalso TTL < M of
		true -> 
			device_get_mttl(W,Rest,TTL);
		false -> 
			device_get_mttl(W,Rest,M)
	end.

device_get_sub(Pattern, Device) ->
	BPattern=case is_binary(Pattern) of
			 true -> Pattern;
			 false -> list_to_binary(Pattern)
		 end,
	BDevice=list_to_binary(integer_to_list(Device)),
	BMatch= <<"worker:",BDevice/binary,":esub:",BPattern/binary,":*">>,
	List=device_get_subi(BMatch, 0),
	MTTL=poolboy:transaction(redis1,
		fun(Worker) -> 
				device_get_mttl(Worker, List, 43200)
		end),
	{ok,[ xsplit(X) || X <- List ],MTTL}.

xsplit(Ident) ->
	[_,_,_,_,ID]=split(Ident,":"),
	ID.

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

