-module(pi_stop).
-export([ds_process/4,ds_process/5,separate/0]).

separate() -> 0.

ds_process(PI_Data, Current, Hist, HState) ->  %{private permanent data, public temporary data proplist}
	ds_process(PI_Data, Current, Hist, HState, []).

ds_process(PI_Data, Current, _Hist, HState, _PI_Param) ->  %{private permanent data, public temporary data proplist}
	{_,[Lon, Lat]}=proplists:lookup(position, Current),
	{_,T}=proplists:lookup(dt,Current),
	{_,Speed}=proplists:lookup(sp,Current),
	Settings=maps:get(settings,HState,[]),
	MinSp=proplists:get_value(minspeed,Settings,1),
	Stop=Speed < MinSp ,
	%lager:info("stop ~p",[dict:to_list(State#state.data)]),
	{LStart,LStop,LState,LProcessed} = case PI_Data of
										   {A,B,C,D}  -> 
											   {A,B,C,D};
										   {_,{A,B,C,D}} -> 
											   {A,B,C,D};
										   {_,{A,B,C,D},_} -> 
											   {A,B,C,D};
										   _ -> 
											   case Stop of
												   true -> {T, T, stop, false};
												   _ -> {T, T, drive, false}
											   end
									   end,

	POIs=proplists:get_value(current_poi,maps:get(pi_poi,HState,[]),[]),
	ID=maps:get(id,HState),
	MStop=proplists:get_value(minstop,Settings,90),
	NSSData=case {Stop == (LState == stop), Stop, LProcessed} of
				{true, true, _} -> %Still stopped
					case LProcessed of
						true -> 
							lager:debug("Car ~p still stopped for ~p, stopped ~p",[ID,T-LStop,LStop]),
							{LStart, LStop, LState, LProcessed};
						false ->
							lager:debug("Stop ~p",[PI_Data]),
							lager:debug("Car ~p stopped for ~p, real stop ~p",[ID,T-LStop,MStop]),
							case T - LStop >= MStop of
								true -> 
									savestop(ID,LStop,LStart,T,stop, {Lon, Lat, POIs}),
									lager:debug("Car ~p Really stopped at ~p",[ID, LStop]),
									{LStart, LStop, LState, true};
								false -> 
									{LStart, LStop, LState, LProcessed}
							end
					end;
				{true, false, _} -> %Still driving, do nothing
					{LStart, LStop, LState, false};
				{false, true, _} -> %just stopped
					lager:debug("Car ~p Drived for a ~p, beginning ~p",[ID,T-LStart,LStart]),
					lager:debug("Car ~p possible stop after ~p",[ID,T-LStart]),
					{LStart, T, stop, false};
				{false, false, true} -> %just started
					lager:debug("Car ~p Stopped for a ~p, stopped ~p",[ID,T-LStop,LStop]),
					savestop(ID,LStop,LStart,T,drive, {Lon, Lat, POIs}),
					lager:debug("Car ~p Start after ~p",[ID,T-LStop]),
					{T, LStop, drive, false};
				{false, false, false} -> %started, but not fully stopped, ignore
					Odo = proplists:get_value(softodometer,Current,0),
					{Prev_T,PLon,PLat,PrevPOI} = case PI_Data of
												   {_,_,{PT,[PPL,PPA],PPOI}} -> 
													   {PT,PPL,PPA,PPOI};
												   _ ->
													   {T-1,Lon,Lat,[]}
											   end,


					%lager:info("Car ~p Stopped for a ~p, odo ~p stopped ~p",[ID,T-LStop,Odo,LStop]),
					lager:debug("Car ~p reStart ~p",[ID,T-LStop]),

					if T-Prev_T>MStop ->
						   AvgSp=Odo/(T-Prev_T),
						   if AvgSp<MinSp ->
								  savestop(ID,Prev_T,LStart,T,stop, {PLon, PLat, PrevPOI}),
								  savestop(ID,Prev_T,T,T,drive, {PLon, PLat, PrevPOI}),
								  lager:info("Car ~p Stopped for a ~p sec, avg spd ~p. 1 point stop",[ID,T-Prev_T,AvgSp]),
								  {T, Prev_T, drive, false};
							  true ->
								  {LStart, LStop, drive, false}
						   end;

					   true ->
						   {LStart, LStop, drive, false}
					end
			end,
	%lager:info("Car ~p L ~p PrData ~p",[State#state.id,LState,{LStart,LStop,LState,LProcessed}]),
	%lager:info("Car ~p L ~p SSData ~p",[State#state.id,LState,NSSData]),
	{
	 {Stop, NSSData, {T,[Lon,Lat],POIs}}, 
	 [
	  {isstop,Stop},
	  {tstart,element(1,NSSData)},
	  {tstop,element(2,NSSData)},
	  {status,element(3,NSSData)},
	  {handled,element(4,NSSData)}
	 ]
	}.

savegk(DeviceID, Hour, Key, Ev, Coords) ->
	JBin=iolist_to_binary(mochijson2:encode(
							[
							 {device,DeviceID},
							 {hour,Hour},
							 {ev,Ev},
							 {coords,Coords},
							 {key,Key}
							])),
	gen_server:cast(redis_set,{cmd, [ "lpush", <<"geocode">>, JBin ]}).	


savestop(DeviceID, LStop, LStart, Now, Sta, {Lon, Lat, POIs}) -> 
	lager:debug("Car ~p Key: ~p",[DeviceID, {type,events, device,DeviceID, hour,trunc(LStop/3600)}]),
	StopH=trunc(LStop/3600),
	KeyS={type,events, device,DeviceID, hour,StopH},
	SKey = list_to_binary("stop."++integer_to_list(LStop)),
	case Sta of
		stop ->
			StartH=trunc(LStart/3600),
			RKey = list_to_binary("drive."++integer_to_list(LStart)),
			case StartH == StopH of
				true ->
					savegk(DeviceID, StartH, SKey, position, [Lon, Lat]),
					savegk(DeviceID, StartH, RKey, eposition, [Lon, Lat]),
%					lager:info("Geocode ~p in ~p ~p and ~p ~p",[{Lon, Lat}, KeyS, SKey,KeyS, RKey]),
					mng:ins_update(mongo,<<"events">>, KeyS, {
														 SKey,{
														   duration, Now-LStop, 
														   fin, 0, 
														   position, [Lon, Lat], 
														   c1, 1,
														   poi, POIs},
														 <<RKey/binary,".duration">>, Now-LStart, 
														 <<RKey/binary,".fin">>, 1, 
														 <<RKey/binary,".eposition">>, [Lon, Lat]
														});
				_ -> 
					%lager:info("Split update1"),
					KeyR={type,events, device,DeviceID, hour,StartH},
					savegk(DeviceID, StopH, SKey, position, [Lon, Lat]),
					savegk(DeviceID, StartH, RKey, eposition, [Lon, Lat]),
%					lager:info("Geocode ~p in ~p ~p and ~p ~p",[{Lon, Lat}, KeyS, SKey, KeyR, RKey]),
					mng:ins_update(mongo,<<"events">>, KeyS, {SKey,{
																duration, Now-LStop, 
																fin, 0, 
																position, [Lon, Lat], 
																c2, 1,
																poi, POIs
															   }
															 }),
					mng:ins_update(mongo,<<"events">>, KeyR, {
														 <<RKey/binary,".duration">>, Now-LStart, 
														 <<RKey/binary,".fin">>, 1, 
														 <<RKey/binary,".eposition">>, [Lon, Lat]
														})
			end;
		drive ->
			NowH=trunc(Now/3600),
			RKey = list_to_binary("drive."++integer_to_list(Now)),
			case NowH == StopH of
				true -> 
					%lager:info("Combine update2"),
					savegk(DeviceID, NowH, RKey, sposition, [Lon, Lat]),
					mng:ins_update(mongo,<<"events">>, KeyS, {
												   <<SKey/binary,".duration">>, Now-LStop,
												   <<SKey/binary,".fin">>, 1,
												   RKey,{
													 duration, Now-LStart, 
													 fin, 0, 
													 sposition, [Lon, Lat]
													}
												  });
				_ ->
					%lager:info("Split update2"),
					KeyR={type,events, device,DeviceID, hour, NowH},
					mng:ins_update(mongo,<<"events">>, KeyS, {
														 <<SKey/binary,".duration">>, Now-LStop, 
														 <<SKey/binary,".fin">>, 1
														}),

%					lager:info("Geocode ~p in ~p ~p",[{Lon, Lat}, KeyR, RKey]),
					savegk(DeviceID, NowH, RKey, sposition, [Lon, Lat]),
					mng:ins_update(mongo,<<"events">>, KeyR, {
														 <<RKey/binary,".duration">>, Now-LStart, 
														 <<RKey/binary,".fin">>, 0, 
														 <<RKey/binary,".sposition">>, [Lon, Lat]
														})
			end;
		_ ->
			ok
	end.


