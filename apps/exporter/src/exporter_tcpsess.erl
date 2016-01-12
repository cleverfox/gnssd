-module(exporter_tcpsess).
-behaviour(ranch_protocol).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/4]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, init/4, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%start_link(ListenerPid, Socket, Transport, Opts) ->
%    gen_server:start_link(?MODULE, [ListenerPid, Socket, Transport, Opts], []).
start_link(Ref, Socket, Transport, Opts) ->
	proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(state,{
		  socket,
		  transport,
		  lasttoken,
		  amqp_conn,
		  amqp_chan,
		  recvd_q=[]
		 }).

-include("deps/rabbitmq-erlang-client/include/amqp_client.hrl").

%application:set_env(exporter,rabbit_host,"fc00:100::5").
%
init([]) ->
	{ok, undefined}.

init(ListenerPid, Socket, Transport, Opts) ->
	ok = proc_lib:init_ack({ok, self()}),
	ok = ranch:accept_ack(ListenerPid),
	ok = Transport:setopts(Socket, [{active, once},{packet,0}]),
	lager:info("Accept ~p ~p ~p ~p",[ListenerPid, Socket, Transport, Opts]),
	gen_server:enter_loop(?MODULE, [],
						  #state{
							 socket=Socket, 
							 transport=Transport,
							 recvd_q=[]
							},
						  10000).

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, <<"quit",_/binary>>}, State=#state{
		socket=Socket, transport=Transport}) ->
	Transport:send(Socket, <<"good bye\r\n">>),
	Transport:close(Socket),
    {stop, normal, State};

handle_info({tcp, Socket, Data}, 
			State=#state{ socket=Socket, transport=Transport}) ->
	lager:info("Recvd ~p",[Data]),
	try 
		[StripRN|_]=binary:split(Data,<<"\r">>,[global]),
		Cmd=case binary:split(StripRN,<<":">>,[global]) of
			[Token, <<"get">> , Arg] ->
				{get,Token,binary_to_integer(Arg)};
			[<<"confirm">>, Arg] ->
				{confirm,binary_to_integer(Arg)};
			[Token, <<"flush">>] ->
				{flush,Token};
			_ ->
				throw(badproto)
		end,
		State2=process_cmd(Cmd,State),
		Transport:setopts(Socket, [{active, once}]),
		{noreply, State2}
	catch throw:badproto ->
			  Transport:send(Socket, <<"bad command\r\n">>),
			  Transport:close(Socket),
			  {stop, normal, State};
		  Ec:Ee ->
			  Transport:send(Socket, <<"broken protocol\r\n">>),
			  Transport:close(Socket),
			  lager:error("Error ~p:~p",[Ec,Ee]),
			  {stop, normal, State}
	end;

handle_info({tcp_closed, Socket}, State=#state{
		socket=Socket}) ->
	{stop, normal, State};

handle_info(_Info, State) ->
	lager:info("Info ~p",[_Info]),
    {noreply, State}.

terminate(_Reason, State) ->
	amqp_channel:close(State#state.amqp_chan),
	amqp_connection:close(State#state.amqp_conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
process_cmd({flush,Chan}, OState) ->
	QName= <<"test_queue">>,
	CState = case OState#state.amqp_chan of
				 undefined -> amqpconnect(OState);
				 _ ->
					 if OState#state.lasttoken == Chan -> OState;
						true -> amqpconnect(OState)
					 end
			 end,
	Declare = #'queue.declare'{queue = QName, durable=true},
	#'queue.declare_ok'{} = amqp_channel:call(CState#state.amqp_chan, Declare),
	Res=doflush(CState#state.amqp_chan, QName, 0),
	ToSend=erlang:iolist_to_binary( io_lib:format("~p~n",[Res])),
	(CState#state.transport):send(CState#state.socket, ToSend),
	CState#state{
	  lasttoken=Chan,
	  recvd_q=[]
	 };


process_cmd({get,Chan, N}, OState) ->
	lager:info("Get ~p ~p",[Chan,N]),
	QName= <<"test_queue">>,
	CState = case OState#state.amqp_chan of
				 undefined -> amqpconnect(OState);
				 _ ->
					 if OState#state.lasttoken == Chan -> OState;
						true -> amqpconnect(OState)
					 end
			 end,
	Declare = #'queue.declare'{queue = QName, durable=true},
	#'queue.declare_ok'{} = amqp_channel:call(CState#state.amqp_chan, Declare),
	R=lists:filtermap(
		fun(_) ->
				Get = #'basic.get'{queue = QName},
				case amqp_channel:call(CState#state.amqp_chan, Get) of
					{#'basic.get_ok'{delivery_tag = Tag}, #amqp_msg{
															 %props=Props,
															 payload=Content
															}} ->
						%amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag});
						{true,{Tag,<<Content/binary,",">>}};
					{'basic.get_empty',<<>>} ->
						false
				end
		end, lists:seq(1,N)),
	{Tags,Data}=lists:unzip(R),
	ToSend= <<"[\n",(erlang:iolist_to_binary(Data))/binary,"null\n]\n">>,
	(CState#state.transport):send(CState#state.socket, ToSend),
	erlang:garbage_collect(self()),
	CState#state{
	  lasttoken=Chan,
	  recvd_q=lists:append(CState#state.recvd_q,Tags)
	 };

process_cmd({confirm, N}, State) ->
	ToSend= <<"ok">>,
	(State#state.transport):send(State#state.socket, ToSend),
	State#state{recvd_q=do_confirm(State#state.recvd_q,N,State#state.amqp_chan)};

process_cmd(Cmd,State) ->
	lager:error("Unhandled command ~p",[Cmd]),
	State.

doflush(Channel, QName, N) ->
	Get = #'basic.get'{queue = QName},
	case amqp_channel:call(Channel, Get) of
		{#'basic.get_ok'{delivery_tag = Tag}, _} ->
			amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
			doflush(Channel, QName, N+1);
		{'basic.get_empty',<<>>} ->
			N
	end.

do_confirm(ConfQ,0,_) ->
	ConfQ;

do_confirm([],_,_) ->
	[];

do_confirm([Tag|Rest],N,Channel) ->
	amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
	do_confirm(Rest,N-1,Channel).

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
	  amqp_chan=Channel,
	  recvd_q=[],
	  lasttoken=undefined
	 }.

