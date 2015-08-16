-module(httprpc).
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

-record(state, {
		  server
		 }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	application:ensure_started(ranch),
	Dispatch = cowboy_router:compile([
									  {'_', [
											 {"/", httprpc_index, []},
											 {"/bapi/near", httprpc_near, []},
											 {"/bapi/path/:device/current", httprpc_curpath, []},
											 {"/api/path/:device/current", httprpc_curpath, []},
											 {"/bapi/test/:device/:t1/:t2", httprpc_notfound, []}
											]}
									 ]),
	cowboy:stop_listener(gnsshttp_listener),
	{ok, Server} = cowboy:start_http(gnsshttp_listener, 100,
								[{port, 8001}],
								[
								 {env, [{dispatch, Dispatch}]},
								 {onresponse, fun error_hook/4}
								]
							   ),
	erlang:link(Server),
%	,
%	case cowboy_clock:start_link() of
%		{ok, _} -> ok;
%		{error,{already_started, _}} -> ok;
%		_Any -> throw(_Any)
%	end

	{ok, #state{ server=Server } }.


handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Info, State) ->
	lager:info("Info ~p",[Info]),
	{noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
	cowboy:stop_listener(gnsshttp_listener),
    ok.

error_hook(404, Headers, <<>>, Req) ->
	Path = cowboy_req:path(Req),
	Body = ["404 Not Found: \"", Path,
		"\" is not the path you are looking for.\n"],
	Headers2 = lists:keyreplace(<<"content-length">>, 1, Headers,
		{<<"content-length">>, integer_to_list(iolist_size(Body))}),
	cowboy_req:reply(404, Headers2, Body, Req);
error_hook(Code, Headers, <<>>, Req) when is_integer(Code), Code >= 400 ->
	Body = ["HTTP Error ", integer_to_list(Code), $\n],
	Headers2 = lists:keyreplace(<<"content-length">>, 1, Headers,
		{<<"content-length">>, integer_to_list(iolist_size(Body))}),
	cowboy_req:reply(Code, Headers2, Body, Req);
error_hook(_Code, _Headers, _Body, Req) ->
	Req.
