-module(httprpc_notfound).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

init(Req, State) ->
	Method = cowboy_req:method(Req),
	QS = cowboy_req:parse_qs(Req),
	P = cowboy_req:path(Req),
	Pi = cowboy_req:bindings(Req),
	L=lists:flatten(io_lib:format("~p",[{P,Pi,QS}])),
	B=list_to_binary(L),
	Body = <<"<h1>404 Not found</h1>",Method/binary,"<br><pre>",B/binary,"</pre>">>,
	Req2 = cowboy_req:reply(404, [
								  {<<"content-type">>, <<"text/html">>}
								 ], Body, Req),

    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.

