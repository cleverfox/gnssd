-module(pi_poi).
-export([ds_process/4,ds_process/5,separate/0]).

separate() -> 0.

ds_process(PI_Data, Current, Hist, HState) ->  %{private permanent data, public temporary data proplist}
	ds_process(PI_Data, Current, Hist, HState, []).

ds_process(PI_Data, Current, _Hist, HState, _PI_Params) ->  %{private permanent data, public temporary data proplist}
	{_,[Lon, Lat]}=proplists:lookup(position, Current),
	{_,T}=proplists:lookup(dt,Current),
	SQL="select id from pois where (organisation_id = $3 or organisation_id = 0) and ST_Intersects(geo,st_makepoint($1,$2))",
	Time1=now(),
	SQLRes=psql:equery(SQL, [Lon,Lat,maps:get(org_id,HState,0)]),
	TimeDiff=timer:now_diff(now(),Time1)/1000,
	if TimeDiff>1000 ->
		   lager:error("POI lookup took ~p ~p",[TimeDiff,[Lon,Lat,maps:get(org_id,HState,0)]]);
	   true ->
		   ok
	end,
	POIs=case SQLRes of
			 {ok,_Hdr,Dat} ->
				 [ X || {X} <- Dat ];
			 _Any -> 
				 []
		 end,
	OLDPois=if is_list(PI_Data) -> PI_Data;
			   true -> []
			end,
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
	
	{POIs,[{current_poi,POIs},{in_poi,POIIn},{out_poi,POIOut}]}.


