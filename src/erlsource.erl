-module(erlsource).

-behaviour(gen_server).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {imeicache}).

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
start_link() ->
    gen_server:start_link({global, gnss_data_source}, ?MODULE, [], []).

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
init([]) ->
	catch register(list_to_atom("erlsource"),self()),
	{ok, #state{imeicache=dict:new()}}.

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
handle_cast({datasource,List}, State) ->
	DevID = case proplists:lookup(imei,List) of
				{imei, IMEIb} ->
					%lager:info("Ok, imei is ~p",[IMEIb]),
					imei2deviceID(binary_to_list(IMEIb),State#state.imeicache);
				_none ->
					lager:info("There is no imei :("),
					{error, false, State}
			end, 
	case DevID of 
		{ok, ID, D2} -> 
			case global:whereis_name({device,ID}) of
				undefined -> 
					case supervisor:start_child(dev_sup,[ID]) of
						{ok,Pid} -> 
							gen_server:cast(Pid,{ds, List});
						Any -> 
							lager:error("Can't start device: ~p",[Any])
					end;
				Pid ->
					gen_server:cast(Pid,{ds, List})
			end,
			{noreply, State#state{imeicache=D2}};
		_ -> 
			{noreply, State}
	end;


handle_cast(stop, State) ->
	{stop, normal, State};

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
%%%
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


