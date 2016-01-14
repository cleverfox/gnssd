-module(httprpc_export_resend).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

init(Req, State) ->
	Token=cowboy_req:binding(token,Req),
	Body=try
			 Ac=case psql:equery("select token from export_tokens where token=$1",[Token]) of
					{ok,_Hdr,[_]} ->
						#{
								imei := BinDev,
								dt_start := BStart,
								dt_end := BEnd
							   } = cowboy_req:match_qs([imei,dt_start,dt_end],Req),
						Start=binary_to_integer(BStart),
						End=binary_to_integer(BEnd),
						case psql:equery("select id from devices where imei=$1",[BinDev]) of
							{ok,_,[{DevID}]} -> 
								E=mng:find(mongo,<<"rawdata">>,
										  {type,rawdata,device,DevID,hour,{'$gte',Start div 3600,'$lte',End div 3600}}),
								Count=sendData(E,Token),
								mc_cursor:close(E),
								Count;
							_ -> 
								throw(unknown_imei)
						end;
						{ok,_Hdr,[]} ->
						throw(token_not_exists);
					_ -> 
						throw(error_get_token)
				end,
			 jsx:encode(#{
			   action => <<"resend">>,
			   token => Token,
			   done => Ac
			  })

		 catch throw:Err ->
				   jsx:encode(#{
					 action => <<"resend">>,
					 token => Token,
					 error => Err
					})
		 end,
	Req2 = cowboy_req:reply(200, 
							[
							 {<<"content-type">>, <<"application/json">>}
							], Body, Req),

	{ok, Req2, State}.

sendData(E, Token) ->
	case mc_cursor:next(E) of
		{MData} ->
			MD=proplists:get_value(raw,mng:m2proplistr(MData)),
			JMD=[ iolist_to_binary(mochijson2:encode( Point )) || Point <- MD ],
			Len=length(JMD),
			gen_server:cast(dexport,{send,[Token],JMD}),
			lager:info("send to ~p ~p",[Token,JMD]),
			Len+sendData(E,Token);
		_ ->
			0
	end.

terminate(_Reason, _Req, _State) ->
	ok.

