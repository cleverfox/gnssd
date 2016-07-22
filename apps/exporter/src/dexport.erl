-module(dexport).
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

-record(state, {
		  amqp_conn,
		  amqp_chan
		 }).

%-include("deps/rabbitmq-erlang-client/include/amqp_client.hrl").
-include("../../deps/amqp_client/include/amqp_client.hrl").
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    {ok, #state{
		  amqp_conn=undefined,
		  amqp_chan=undefined
		   }
	}.

handle_call({flush,Chan}, _From, State) ->
	QName= <<"export.",Chan/binary>>,
	try
		Res=doflush(State#state.amqp_chan, QName, 0),
		{reply, {ok, Res}, State}
	catch Ec:Ee ->
			  lager:info("Can't flush ~p:~p",[Ec,Ee]),
			  {reply, {error, unknown}, State}
	end;

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast({send,Channels, DS}, OState) ->
	State = case OState#state.amqp_chan of
				 undefined -> amqpconnect(OState);
				 _ ->
					 OState
			 end,
	lists:foreach(fun(Channel) ->
						  Queue= <<"export.",Channel/binary>>,
						  Declare = #'queue.declare'{queue = Queue, durable=true},
						  #'queue.declare_ok'{} = amqp_channel:call(State#state.amqp_chan, Declare),
						  Publish = #'basic.publish'{ exchange = <<>>, routing_key = Queue },
						  lists:foreach(
							fun(Payload) ->
									amqp_channel:cast(State#state.amqp_chan, Publish, 
													  #amqp_msg{
														 payload = Payload,
														 props=#'P_basic'{
																  content_type = <<"json">>,
																  timestamp = os:system_time(),
																  delivery_mode = 2
																 }
														}),
									lager:info("Push ~p~n",[Payload])
							end,DS)
				  end, Channels),



	{noreply, State};

handle_cast(_Msg, State) ->
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

amqpconnect(State) ->
	if State#state.amqp_chan == undefined -> ok;
	   true ->
		   ok=amqp_channel:close(State#state.amqp_chan),
		   ok=amqp_connection:close(State#state.amqp_conn)
	end,

	application:ensure_all_started(amqp_client),
	{ok, Connection} = amqp_connection:start(#amqp_params_network{
												host="fc00:100::5"
												%host="100.64.20.5"
											   }),
	{ok, Channel} = amqp_connection:open_channel(Connection),
	State#state{
	  amqp_conn=Connection,
	  amqp_chan=Channel
	 }.

doflush(Channel, QName, N) ->
	Get = #'basic.get'{queue = QName},
	case amqp_channel:call(Channel, Get) of
		{#'basic.get_ok'{delivery_tag = Tag}, _} ->
			amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
			doflush(Channel, QName, N+1);
		{'basic.get_empty',<<>>} ->
			N
	end.

