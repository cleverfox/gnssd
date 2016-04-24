-module(httprpc_fuelhint).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

init(Req, State) ->
	Dev=binary_to_integer(cowboy_req:binding(device,Req)),
	Hour=binary_to_integer(cowboy_req:binding(hour,Req)),

	Ms=try
		{HData} = mng:find_one(mongo,
							   <<"devicedata">>,
							   {device,Dev,hour,Hour,type,devicedata},
							   {'aggregated.agg_fuelgauge',1,'_id',0}
							  ),
			proplists:get_value(agg_fuelgauge,
								proplists:get_value(aggregated,
													mng:m2proplistr(HData)
												   )
							   )
	catch _:_ -> #{}
	end,

	{HintID,Hints}=try
			  {HData1} = mng:find_one(mongo,
									  <<"hints">>,
									  {device,Dev,hour,Hour,type,hints},
									  {'hints.fuel',1,'_id',1}
									 ),

			  MPL=mng:m2proplistr(HData1),
			  {
			   proplists:get_value('_id', MPL),
			   proplists:get_value(fuel,
								  proplists:get_value(hints, MPL)
								 )
			  }
		  catch _:_ -> {undefined,#{}}
		  end,
	Body=case cowboy_req:method(Req) of
			 <<"POST">> -> 
				 Request=case cowboy_req:body(Req) of
							 {ok, Data, _} -> jsx:decode(Data);
							 {more, Data, _} -> jsx:decode(Data);
							 {error, _Reason} -> <<"">>
						 end,
				 lager:info("Request ~p",[Request]),
				 
				 ResID=mng:ins_update(mongo,
								<<"hints">>,
								{device,Dev,hour,Hour,type,hints},
								{hints,{fuel,Request}}),
				 <<(jsx:encode(#{
					  req=>Request,
					  hint1=>hint2list(HintID),
					  hint2=>hint2list(ResID)
					  %fueldata=>Ms,
					  %hints=>Hints,
					  %device_id=>Dev,
					  %hour=>Hour,
					  %start=>Hour*3600,
					  %stop=>(Hour+1)*3600,
					  %msg=><<"Preved">>
					 }))/binary,"\n">>;
			 Method -> <<(jsx:encode(#{
							m => Method,
							fueldata=>Ms,
							hints=>Hints,
							device_id=>Dev,
							hour=>Hour,
							start=>Hour*3600,
							stop=>(Hour+1)*3600,
							msg=><<"Preved">>
						   }))/binary,"\n">>
		 end,

	Req2 = cowboy_req:reply(200, [
								  {<<"content-type">>, <<"application/json">>}
								 ], Body, Req),

	{ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
	ok.

hint2list(X) when is_binary(X) ->
	binary_to_list(X);

hint2list(_) ->
	null.
