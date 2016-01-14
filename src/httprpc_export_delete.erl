-module(httprpc_export_delete).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

init(Req, State) ->
	Token=cowboy_req:binding(token,Req),
	Ac=case psql:equery("select token from export_tokens where token=$1",[Token]) of
		{ok,_Hdr,[_]} ->
			   case psql:equery("delete from export_tokens where token=$1;",[Token]) of
				   {ok, 1} ->
					   ok;
				   _ ->
					   error
			   end;
		{ok,_Hdr,[]} ->
			   not_exists;
		_ -> error
	end,
	Body=jsx:encode(#{
		   action => <<"delete">>,
		   token => Token,
		   status => Ac
		  }),
	Req2 = cowboy_req:reply(200, 
							[
							 {<<"content-type">>, <<"application/json">>}
							], Body, Req),

    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.

