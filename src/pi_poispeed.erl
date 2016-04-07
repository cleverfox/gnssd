-module(pi_poispeed).
-export([ds_process/4,ds_process/5,separate/0]).
-include("include/device.hrl").

-export([get_poispeed/2,poi_speedcache/3,min_speed/3, test/0]).
separate() -> 0.

ds_process(PI_Data, Current, Hist, HState) ->  %{private permanent data, public temporary data proplist}
	ds_process(PI_Data, Current, Hist, HState, []).

ds_process(PI_Data0, Current, _Hist, HState, _PI_Params) ->  %{private permanent data, public temporary data proplist}
	PI_Data=if is_map(PI_Data0) ->
				   PI_Data0;
			   true -> #{}
			end,
%	Dev=maps:get(id,HState),
	T=proplists:get_value(dt,Current),
	CSpeed=proplists:get_value(sp,Current),
	Org=maps:get(org_id,HState,0),
	PI_POI_Info=maps:get(pi_poi,HState,[]),
	Current_POIs=proplists:get_value(current_poi,PI_POI_Info,[]),
	Cache=maps:get(poicache, PI_Data, #{}),
	{MaxSpeed,Cache2} = min_speed(Current_POIs, Org, Cache),
	OverSpeed=CSpeed>=MaxSpeed,
	PreOverSpeed=maps:get(overspeed, PI_Data, false),
	{Begin,MaxSpeeding,Finished}=if OverSpeed andalso not PreOverSpeed ->
							%just begin
							{
							 T, %begin timestamp
							 CSpeed, %max_speed
							 maps:get(finished_overspeed, PI_Data, undefined)
							};
						not OverSpeed andalso PreOverSpeed ->
							%finished,
							{
							 undefined,
							 0,
							 {
							  maps:get(begin_over, PI_Data, undefined),
							  T,
							  maps:get(max_speeding, PI_Data, undefined)
							 }
							};
						true ->
							%nothing changed
							{
							 maps:get(begin_over, PI_Data, undefined),
							 if OverSpeed ->
								   case maps:get(max_speeding, PI_Data, undefined) of
									   I when is_integer(I) andalso I > CSpeed ->
										   I;
									   _ -> CSpeed
								   end;
							   true ->
								   undefined
							 end,
							 maps:get(finished_overspeed, PI_Data, undefined)
							}
					 end,
	{
	 PI_Data#{ 
	   begin_over => Begin,
	   overspeed => OverSpeed,
	   max_speeding => MaxSpeeding,
	   finished_overspeed => Finished,
	   poicache => Cache2
	  }, 
	 [
	  {max_speed, MaxSpeed},
	  {overspeed, OverSpeed},
	  {max_speeding, MaxSpeeding},
	  {overspeed_last, Finished},
	  {overspeed_since, Begin}
	 ]
	}.


savegk(ID, Ev, Coords) ->
	try
		lager:info("Geocode ~p ~p ~p",[ID, Ev, Coords]),
		JBin=iolist_to_binary(mochijson2:encode(
								[
								 {id,mng:id2hex(ID)},
								 {collection, <<"ibutton">>},
								 {ev,Ev},
								 {coords,Coords}
								])),
		lager:info("JBIN ~p",[JBin]),
		gen_server:cast(redis_set,{cmd, [ "lpush", <<"geocode">>, JBin ]})
	catch _:_ ->
			  ok
	end.

min_speed([], _Org, Cache, MinSP) ->
	{MinSP, Cache};

min_speed([POI|POIs], Org, Cache, MinSP) ->
	{PSpeed, Cache2} = poi_speedcache(POI, Org, Cache),
	NewMinSP=case PSpeed of 
				 unlimited -> MinSP;
				 Limit ->
					 if MinSP == unlimited ->
							Limit;
						Limit < MinSP ->
							Limit;
						true ->
							MinSP
					 end
			 end,
	min_speed(POIs, Org, Cache2, NewMinSP).

min_speed(POIs, Org, Cache) ->
	min_speed(POIs, Org, Cache, unlimited).

poi_speedcache(POI, Org, Cache) ->
	T=time_compat:os_system_time(seconds),
	case maps:get(POI, Cache, undefined) of
		{Limit, TExp} when TExp > T ->
			{Limit, Cache};
		_Any -> 
			Limit=get_poispeed(POI, Org),
			{Limit, maps:put(POI, {Limit, T+3600}, Cache)}
	end.


get_poispeed(POI, Org) ->
	SQL="select max_speed from poi_speed_limits where poi_id = $1 and (organisation_id=$2 or organisation_id=0) order by organisation_id desc limit 1;",
	SQLRes=psql:equery(SQL, [POI,Org]),
	case SQLRes of
		{ok,_Hdr,[{Speed}]} ->
			Speed;
		{ok,_Hdr, []} -> 
			unlimited
	end.


test() ->
	{S0,_}=pi_poispeed:ds_process(0,[{dt,100},{sp,110}],[],#{pi_poi=>[{current_poi,[356]}],org_id=>1},[]),
	{S1,_}=pi_poispeed:ds_process(S0,[{dt,200},{sp,110}],[],#{pi_poi=>[{current_poi,[357]}],org_id=>1},[]),
	{S2,_}=pi_poispeed:ds_process(S1,[{dt,300},{sp,160}],[],#{pi_poi=>[{current_poi,[357]}],org_id=>1},[]),
	{S3,_}=pi_poispeed:ds_process(S2,[{dt,400},{sp,190}],[],#{pi_poi=>[{current_poi,[357]}],org_id=>1},[]),
	{S4,_}=pi_poispeed:ds_process(S3,[{dt,500},{sp,110}],[],#{pi_poi=>[{current_poi,[357,356]}],org_id=>1},[]),
	pi_poispeed:ds_process(S4,[{dt,800},{sp,110}],[],#{pi_poi=>[{current_poi,[356]}],org_id=>1},[]).
