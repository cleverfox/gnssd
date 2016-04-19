-module(txtresolver).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({push, Channel, EData} , State) when is_list(EData) ->
	PF=fun(Worker) ->
			   ED2=case proplists:get_value(device_id,EData) of
					   I when is_integer(I) ->
						   [{device_title,device_process(Worker,I)}|EData];
					   _ ->
						   EData
				   end,
			   ED3=case proplists:get_value(pois,ED2) of
					   POIs when is_list(POIs) ->
						   NoPOI=lists:keydelete(pois,1,ED2),
						   Pl=lists:map(fun(POI) when is_integer(POI) ->
												{POI,poi_process(Worker,POI)} ;
										   (Any) ->
												{0, Any}
										end, POIs),
						   [{pois, Pl } | NoPOI] ;
					   _ ->
						   ED2
				   end,
			   ED4=case proplists:get_value(ib,ED3) of
					   IBs when is_list(IBs) ->
						   NoIB=lists:keydelete(ib,1,ED3),
						   IBl=lists:map(fun(IB) when is_list(IB) ->
												 case {proplists:get_value(kind,IB),proplists:get_value(id,IB)} of
													 {<<"driver">>,DID} ->
														 lager:info("driver IB ~p",[IB]),
														 [{driver,[
																   {id, DID}
																  ]
														  }]++IB;
													 {<<"trailer">>,DID} ->
														 lager:info("trailer IB ~p",[IB]),
														 [{trailer,[
																	{id, DID}
																   ]
														  }]++IB;
													 _ ->
														 lager:info("x3 IB ~p",[IB]),
														 IB
												 end;
											(Any) ->
												 Any
										 end, IBs),
						   lager:info("IB2 ~p",[IBl]),
						   [{ibs, IBl } | NoIB];
					   _ ->
						   ED3
				   end,
			   Res=jsx:encode(ED4),
			   lager:info("Data1 ~p",[ EData ]),
			   %lager:info("Data2 ~p",[ ED4 ]),
			   %lager:info("Data3 ~p",[ Res ]),
			   gen_server:cast(redis2nginx,{push,Channel,Res})
	   end,

	poolboy:transaction(postgres, PF),
	{noreply, State};

handle_cast(_Msg, State) ->
	lager:error("Unknown command ~p",[_Msg]),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

poi_process(Worker,I) ->
	SQLRes=gen_server:call(Worker, {equery, "select title from pois where id=$1", [I]}, 10000),
	lager:info("~p",[SQLRes]),
	%list_to_binary("poi"++integer_to_list(I)).
	case SQLRes of 
		{ok,_,[{Name}]} -> Name;
		_ -> "??? unknown ???"
	end.

device_process(Worker,I) ->
	SQLRes=gen_server:call(Worker, {equery, "select title from devices where id=$1", [I]}, 10000),
	lager:info("~p",[SQLRes]),
	%list_to_binary("dev"++integer_to_list(I)).
	case SQLRes of 
		{ok,_,[{Name}]} -> Name;
		_ -> "??? unknown ???"
	end.


