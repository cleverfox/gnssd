-module(httprpc_index).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

init(Req, State) ->
    Body = <<"<h1>It works!</h1>">>,
    Req2 = cowboy_req:reply(200, [
								  {<<"content-type">>, <<"text/html">>}
								 ], Body, Req),
    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.

