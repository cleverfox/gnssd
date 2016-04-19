-module(pi_poispeed).
-export([ds_process/4,ds_process/5,separate/0]).
-include("include/device.hrl").

-export([get_poispeed/2,poi_speedcache/3,min_speed/3, test/0]).
separate() -> 0.

ds_process(PI_Data, Current, Hist, HState) ->  %{private permanent data, public temporary data proplist}
	ds_process(PI_Data, Current, Hist, HState, []).

ds_process(PI_Data0, Current, _Hist, HState, _PI_Params) ->  %{private permanent data, public temporary data proplist}
%	lager:info("-----[ ~p ]-----",[?MODULE]),
	PI_Data=if is_map(PI_Data0) ->
				   PI_Data0;
			   true -> #{}
			end,
	DevID=maps:get(id,HState),
	T=proplists:get_value(dt,Current),
	CSpeed=proplists:get_value(sp,Current),
	[Lon, Lat]=proplists:get_value(position, Current),
	Org=maps:get(org_id,HState,0),
	PI_POI_Info=maps:get(pi_poi,HState,[]),
	Current_POIs=proplists:get_value(current_poi,PI_POI_Info,[]),
	Overspeeds=maps:get(overspeeds, PI_Data, #{}),
	{POI_Speeds, Cache2} = lists:foldl(fun(POI, {PRes, Acc}) ->
											   {PSpeed, Acc2} = poi_speedcache(POI, Org, Acc),
											   {
												if PSpeed == unlimited -> PRes;
												   true -> [{POI, PSpeed}|PRes]
												end,
												Acc2
											   }
									   end, 
									   {[], maps:get(poicache, PI_Data, #{})}, 
									   Current_POIs),
	Prev_POIs=maps:keys(Overspeeds),
	ToCheck=lists:usort(Prev_POIs++Current_POIs),
	CheckPOIFun=fun(POI,{PRes, PState}) ->
						Pre=maps:get(POI,Overspeeds,undefined),
						Currently=lists:keyfind(POI,1,POI_Speeds),
						{POIState, 
						 Speeding, 
						 Ev} = case {Pre, Currently} of
								   {undefined, {POI, PSpeed}} -> %just appear
									   NowSpeeding=CSpeed > PSpeed,
									   {
										maps:put(speeding,NowSpeeding,#{}), 
										{false, NowSpeeding}, in
									   };
								   {_, {POI, PSpeed}} -> %still exists
									   PS=maps:get(POI,PState,#{}),
									   NowSpeeding=CSpeed > PSpeed,
									   {
										maps:put(speeding,NowSpeeding,PS), 
										{maps:get(speeding, PS, false), NowSpeeding}, still
									   };
								   {_, false} -> %just disappear
									   PS=maps:get(POI,PState,#{}),
									   {
										PS,
										{maps:get(speeding, PS, false), false}, out
									   }
							   end,
						{NewPRes,NewPOIState}=case Speeding of
												  {false, true} -> %start speeding
													  saveoverspeed(DevID, beg, T, undefined, [Lon,Lat], CSpeed),
													  {
													   [{POI, proplists:get_value(POI,POI_Speeds),start,
														 #{ max => CSpeed }
														}|PRes], 
													   maps:put(sp_begin,T, 
																maps:put(maxspeed,CSpeed,POIState)
															   )
													  };
												  {true, false} -> %end speeding
													  saveoverspeed(DevID, fin, maps:get(sp_begin, POIState, undefined), T, 
																	[Lon,Lat], maps:get(maxspeed, POIState, undefined)),
													  {
													   [{POI,proplists:get_value(POI,POI_Speeds),fin, 
														 #{ max => maps:get(maxspeed, POIState, undefined),
															since => maps:get(sp_begin, POIState, undefined)
														  } 
														}|PRes], 
													   POIState
													  };
												  {true, true} -> %still speeding
													  PreSpeed=maps:get(maxspeed,POIState,0),
													  MaxSpeed=if(PreSpeed < CSpeed) -> CSpeed;
																 true -> PreSpeed
															   end,
													  {
													   [{POI,proplists:get_value(POI,POI_Speeds), speeding,
														 #{ max => maps:get(maxspeed, POIState, undefined),
															since => maps:get(sp_begin, POIState, undefined)
														  } 
														}|PRes], 
													   maps:put(maxspeed, MaxSpeed, POIState)
													  };
												  {false, false} -> %not speeding
													  {
													   if Currently == false -> %leave poi
															  PRes;
														  true -> 
															  [{POI,proplists:get_value(POI,POI_Speeds),false,#{}}|PRes]
													   end, 
													   POIState
													  }
											  end,

						{
						 NewPRes, 
						 if Ev == out -> maps:remove(POI,PState);
							true -> maps:put(POI,NewPOIState,PState)
						 end
						}
				end,
	{Results, NewState} = lists:foldl(CheckPOIFun, {[], Overspeeds}, ToCheck),
	{
	 PI_Data#{ 
	   poicache => Cache2,
	   overspeeds => NewState
	  }, 
	 Results
	}.

saveoverspeed(DevID,beg,T1,_T2,[Lon,Lat],MaxSpeed) ->
	R=mevent:saveevent(DevID,{
					   list_to_binary("overspeed."++integer_to_list(T1)++".begin"), 
					   {
						dt,T1,
						position,[Lon,Lat]
					   },
					   list_to_binary("overspeed."++integer_to_list(T1)++".maxspeed"), 
					   MaxSpeed
					  },T1),
	lager:info("SaveOverspeed ~p=~p",[{DevID,beg,T1,_T2,[Lon,Lat],MaxSpeed},R]),
					  ok;

saveoverspeed(DevID,fin,T1,T2,[Lon,Lat],MaxSpeed) ->
	R=mevent:saveevent(DevID,{
						 list_to_binary("overspeed."++integer_to_list(T1)++".finish"), 
						 {
						  dt,T2,
						  position,[Lon,Lat]
						 },
						 list_to_binary("overspeed."++integer_to_list(T1)++".maxspeed"), 
						 MaxSpeed
						},T1),
	lager:info("SaveOverspeed ~p=~p",[{DevID,fin,T1,T2,[Lon,Lat],MaxSpeed},R]),
	savegk(R, list_to_binary("overspeed."++integer_to_list(T1)++".finish.position_txt"), [Lon,Lat]),
	ok.


savegk(ID, Ev, Coords) ->
	try
		lager:info("Geocode ~p ~p ~p",[ID, Ev, Coords]),
		JBin=iolist_to_binary(mochijson2:encode(
								[
								 {id,mng:id2hex(ID)},
								 {collection, <<"events">>},
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
	P=[{position,[36,51]}],
	{S0,T0}=pi_poispeed:ds_process(0,[{dt,100},{sp,110}|P],[],#{pi_poi=>[{current_poi,[356]}],org_id=>1,id=>100500},[]),
	{S1,T1}=pi_poispeed:ds_process(S0,[{dt,200},{sp,110}|P],[],#{pi_poi=>[{current_poi,[357]}],org_id=>1,id=>100500},[]),
	{S2,T2}=pi_poispeed:ds_process(S1,[{dt,300},{sp,160}|P],[],#{pi_poi=>[{current_poi,[357]}],org_id=>1,id=>100500},[]),
	{S3,T3}=pi_poispeed:ds_process(S2,[{dt,400},{sp,190}|P],[],#{pi_poi=>[{current_poi,[357]}],org_id=>1,id=>100500},[]),
	{S4,T4}=pi_poispeed:ds_process(S3,[{dt,500},{sp,110}|P],[],#{pi_poi=>[{current_poi,[357,356]}],org_id=>1,id=>100500},[]),
	{S5,T5}=pi_poispeed:ds_process(S4,[{dt,800},{sp,110}|P],[],#{pi_poi=>[{current_poi,[356]}],org_id=>1,id=>100500},[]),
	io:format("T0 ~p~n",[T0]),
	io:format("T1 ~p~n",[T1]),
	io:format("T2 ~p~n",[T2]),
	io:format("T3 ~p~n",[T3]),
	io:format("T4 ~p~n",[T4]),
	io:format("T5 ~p~n",[T5]),
	S5.

