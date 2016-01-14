-module(httprpc_export_unsubscribe).
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
			   #{imeis := Devs } = cowboy_req:match_qs([imeis],Req),
			   Devices=lists:map(fun(BinDev) ->
								 case psql:equery("select id from devices where imei=$1",[BinDev]) of
									 {ok,_,[{DevID}]} -> 
										 case psql:equery("select count(*) from export_subs where token=$1 and device_id=$2",[Token,DevID]) of
											 {ok, _, [{1}]} -> 
												 case psql:equery("delete from export_subs where token=$1 and device_id=$2",[Token,DevID]) of
													 {ok, _} -> 
														 gen_server:cast(redislsource,{reload,BinDev}),
														 {BinDev, done};
													 _ -> {BinDev, error}
										 end;

											 _ -> {BinDev, not_exists}
										 end;
									 _ -> 
										 {BinDev, unknown_imei}
								 end
						 end, binary:split(Devs,<<",">>,[global])),
			   Devices;
		{ok,_Hdr,[]} ->
			   token_not_exists;
		_ -> error
	end,
	Body=jsx:encode(#{
		   action => <<"add_imei">>,
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

