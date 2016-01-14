-module(httprpc_export_flush).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

init(Req, State) ->
	Token=cowboy_req:binding(token,Req),
	Body=case gen_server:call(dexport,{flush, Token}) of
			 {ok, N} ->
				 jsx:encode(#{
				   action => <<"flush">>,
				   token => Token,
				   result => ok,
				   flushed => N 
			  });
			 _ -> 
				 jsx:encode(#{
				   action => <<"flush">>,
				   token => Token,
				   result => error
			  })
		 end,
		Req2 = cowboy_req:reply(200, [
								  {<<"content-type">>, <<"application/json">>}
								 ], Body, Req),

    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.

