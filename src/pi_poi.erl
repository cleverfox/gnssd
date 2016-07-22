-module(pi_poi).
-export([ds_process/4,ds_process/5,separate/0]).

separate() -> 0.

ds_process(PI_Data, Current, Hist, HState) ->  %{private permanent data, public temporary data proplist}
	ds_process(PI_Data, Current, Hist, HState, []).

ds_process(PI_SRCData, Current, _Hist, HState, _PI_Params) ->  %{private permanent data, public temporary data proplist}
	{_,[Lon, Lat]}=proplists:lookup(position, Current),
	{_,T}=proplists:lookup(dt,Current),
	{PI_Data,PI_Cache}=case PI_SRCData of
						   {A,B} when is_list(A) andalso is_map(B) ->
							   {A,B};
						   A when is_list(A) ->
							   {A, #{}};
						   _ ->
							   {[],#{}}
					   end,

	OLDPois=if is_list(PI_Data) -> PI_Data;
			   true -> []
			end,

	{POIs, PI_Cache2} = do_lookup(maps:get(org_id,HState,0), Lon, Lat, PI_Cache, OLDPois),

	POIIn=lists:subtract(POIs,OLDPois),
	POIOut=lists:subtract(OLDPois,POIs),
	case length(POIIn)>0 of
		true ->
			mevent:saveevent(maps:get(id,HState),{ 
									list_to_binary("poi.enter."++integer_to_list(T)), POIIn 
									,
									list_to_binary("poi.list."++integer_to_list(T)), POIs 
								   },T);
		_ -> ok
	end,
	case length(POIOut)>0 of
		true -> 
			mevent:saveevent(maps:get(id,HState),{
									list_to_binary("poi.leave."++integer_to_list(T)), POIOut 
									,
									list_to_binary("poi.list."++integer_to_list(T)), POIs 
								   },T);
		_ -> ok
	end, 
	
	{{POIs,PI_Cache2},[{current_poi,POIs},{in_poi,POIIn},{out_poi,POIOut}]}.

do_lookup(Org, Lon, Lat, Cache0, OLDPois) ->
	try 
	{Cache, Found} = get_cache(Lon, Lat, Cache0),
	case Found of
		undefined ->
			POIs=case poi_lookup:lookup(Org,Lon,Lat) of
					 {error, _} -> OLDPois;
					 {timeout, _} -> OLDPois;
					 {ok, List} -> List
				 end,
			{POIs, put_cache(Lon, Lat, POIs, Cache)};
		POIs ->
			{POIs, Cache}
	end
	catch _Ec:_Ee ->
			  Stack=erlang:get_stacktrace(),
			  lager:error("poi_lookup ochen error ~p:~p at ~p",
						  [_Ec, _Ee, Stack]),
			  POIs2=case poi_lookup:lookup(Org,Lon,Lat) of
					   {timeout, _} -> OLDPois;
					   {ok, List2} -> List2
				   end,
			  {POIs2, Cache0}
	end.

check_hour(Lon, Lat, H, C) ->
	maps:get({Lon, Lat}, maps:get(H, C, #{}), undefined).

get_cache(Lon, Lat, Cache0) ->
	H=os:system_time(seconds) div 3600,
	Cache=maps:filter(
			fun(CH,_) when CH==H orelse CH==H-1 -> true;
			   (_,_) -> false
			end, Cache0),
	{Cache,
	 case check_hour(Lon, Lat, H, Cache) of
		 undefined -> 
			 check_hour(Lon, Lat, H-1, Cache);
		 Any -> Any
	 end
	}.

put_cache(Lon, Lat, POIs, Cache0) ->
	H=os:system_time(seconds) div 3600,
	maps:put(
	  H, 
	  maps:put({Lon, Lat}, POIs, 
			   maps:get(H, Cache0, #{})
			  ), 
	  Cache0
	 ).


