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
	Org=b2f(proplists:get_value(<<"org">>,Params,<<"0">>)),
	Lon=b2f(proplists:get_value(<<"lon">>,Params,<<"0">>)),
	Lat=b2f(proplists:get_value(<<"lat">>,Params,<<"0">>)),
	Dist=b2f(proplists:get_value(<<"dist">>,Params,<<"5000">>))/1000,
	T=b2f(proplists:get_value(<<"t">>,Params,<<"0">>)),
	LKey={type,locations, organisation_id,Org, day,trunc(T/86400)}, 
	HH=trunc(T/1800),
	H=trunc(T/3600),
	N1=erlang:now(),
	PL=case mng:find_one(mongo,<<"locations">>,LKey,{'_id',0,integer_to_binary(HH),1}) of
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
	FindNear=fun(P,P1,P2) ->
					 if P < P1 -> P1;
						P > P2 -> P2;
						true ->
							DP1 = P-P1,
							DP2 = P2-P,
							if DP1>DP2 -> P1;
							   true -> P2
							end
					 end
			 end,

	PL1=lists:filtermap(fun({Dev,{X1,X2,Y1,Y2}}) ->
								CX=FindNear(Lon, X1, X2),
								CY=FindNear(Lat, Y1, Y2),
								D=geotools:dist_simple({CX,CY},{Lon,Lat}),
								%lager:info("~p",[{X1,X2,Y1,Y2}]),
								%lager:info("cx ~p cy ~p D ~p",[CX,CY,D]),
								if Dist>=D ->
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
	N2=erlang:now(),
	TMS=timer:now_diff(N2,N1)/1000,
	lager:info("PL ~p ~p",[TMS,PL1]),
	B=#{
	  org_id=>Org,
	  lon=>Lon,
	  lat=>Lat,
	  dist=>Dist,
	  near=>PL1,
	  time_ms=>TMS,
	  t=>T
	 },
	lager:info("B ~p",[B]),
	Body=jsx:encode(B),
	Req2 = cowboy_req:reply(200, 
							[
							 {<<"content-type">>, <<"application/json">>}
							], Body, Req),

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

