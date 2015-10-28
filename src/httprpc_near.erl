-module(httprpc_near).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

b2f(Bin) ->
   try 	
	   binary_to_float(Bin)
   catch error:badarg ->
			 binary_to_integer(Bin)
   end.


init(Req, State) ->
	Params=cowboy_req:parse_qs(Req),
	FetchFun=fun(_) ->
					 Org=b2f(proplists:get_value(<<"org">>,Params,<<"0">>)),
					 Lon=b2f(proplists:get_value(<<"lon">>,Params,<<"0">>)),
					 Lat=b2f(proplists:get_value(<<"lat">>,Params,<<"0">>)),
					 Dist=b2f(proplists:get_value(<<"dist">>,Params,<<"5000">>))/1000,
					 T=b2f(proplists:get_value(<<"t">>,Params,<<"0">>)),
					 LKey={type,locations, organisation_id,Org, day,trunc(T/86400)}, 
					 HH=trunc(T/1800),
					 H=trunc(T/3600),
					 N1=time_compat:erlang_system_time(micro_seconds),
					 MNG1=mng:find_one(mongo,<<"locations">>,LKey,{'_id',0,integer_to_binary(HH),1}),
					 PL=case MNG1 of
							{{_,L}} ->
								lists:map(fun({DevIDa, Tuple}) when is_atom(DevIDa) andalso is_tuple(Tuple) ->
												  Lst=mng:m2proplist(Tuple),
												  {
												   list_to_integer(atom_to_list(DevIDa)), 
												   { 
													proplists:get_value(x1, Lst),
													proplists:get_value(x2, Lst),
													proplists:get_value(y1, Lst),
													proplists:get_value(y2, Lst)
												   }
												  }
										  end,
										  mng:m2proplist(L));
							_ -> []
						end,
					 PL1=lists:filtermap(fun({Dev,{X1,X2,Y1,Y2}}) ->
												 Matched = Dist>=boundbox:ppdist(X1,X2,Y1,Y2,Lon,Lat),
												 if Matched ->
														case mng:find_one(mongo,<<"devicedata">>,
																		  {type,devicedata,device,Dev,hour,H},
																		  {'_id',0,'data.dt',1,'data.position',1}) of
															{{data,DL0}} ->
																{DT,[FLon, FLat]}=filterDD(T,DL0),
																DDD=geotools:dist_simple({Lon,Lat},{FLon,FLat}),
																%lager:info("Dev ~p DL ~p ~p ~p ~p",[Dev, DT, FLon, FLat,DDD]),
																if Dist >= DDD ->
																	   {true, #{
																		  device=>Dev, 
																		  dt=>DT, 
																		  position=>[FLon, FLat],
																		  dist=>trunc(DDD*1000)
																		 }
																	   };
																   true ->
																	   false
																end;
															_ -> 
																false
														end;
													true ->
														false
												 end
										 end, PL),
					 N2=time_compat:erlang_system_time(micro_seconds),
					 TMS=N2-N1/1000,
					 lager:info("Time_all ~p ms",[TMS]),
					 %lager:info("PL ~p ~p",[TMS,PL1]),
					 B=#{
					   org_id=>Org,
					   lon=>Lon,
					   lat=>Lat,
					   dist=>Dist,
					   near=>PL1,
					   time_ms=>TMS,
					   t=>T
					  },
					 %lager:info("B ~p",[B]),
					 jsx:encode(B) 
			 end,
	Req2=try 
			 Cachekey = <<
						  "cache:nearpos:",
						  (proplists:get_value(<<"org">>,Params,<<"0">>))/binary,":",
						  (proplists:get_value(<<"lon">>,Params,<<"0">>))/binary,":",
						  (proplists:get_value(<<"lat">>,Params,<<"0">>))/binary,":",
						  (proplists:get_value(<<"dist">>,Params,<<"5000">>))/binary,":",
						  (proplists:get_value(<<"t">>,Params,<<"0">>))/binary
						>>,

			 Body=case proplists:get_value(<<"nocache">>,Params,<<"0">>) of
					  <<"1">> ->
						  FetchFun(x);
					  _ ->
						  rcache:cget(Cachekey,FetchFun, #{expire=>86400})
				  end,
			 cowboy_req:reply(200, 
							  [
							   {<<"content-type">>, <<"application/json">>}
							  ], Body, Req)

		 catch Ec:Ee ->
				   ST=iolist_to_binary(lists:map(fun(E)->
														 io_lib:format("At ~p~n",[E])
												 end,erlang:get_stacktrace())),
				   ErrB=iolist_to_binary(io_lib:format("~p~n",[{Ec,Ee}])),

				   cowboy_req:reply(500, 
									[
									 {<<"content-type">>, <<"text/plain">>}
									], 
									<<ErrB/binary,"\n",ST/binary>>,
									Req)
		 end,
		{ok, Req2, State}.


filterDD(T,Data) -> 
	filterDD(T,Data,{0,[0,0]}).

filterDD(_T,[],Last) -> Last;
filterDD(T,[{dt,DT,position,Pos}|Rest],{PT,PP}) -> 
	if DT==T ->
		   {DT,Pos};
	   DT<T ->
		   filterDD(T,Rest,{DT,Pos});
	   T-DT >= T-PT -> 
		   {PT, PP};
	   true ->
		   {DT, Pos}
	end.

terminate(_Reason, _Req, _State) ->
	ok.

