-module(pi_stop).
-export([ds_process/4,ds_process/5]).

ds_process(PI_Data, Current, Hist, HState) ->  %{private permanent data, public temporary data proplist}
	ds_process(PI_Data, Current, Hist, HState, []).

ds_process(PI_Data, Current, _Hist, HState, _PI_Param) ->  %{private permanent data, public temporary data proplist}
	{_,[Lon, Lat]}=proplists:lookup(position, Current),
	{_,T}=proplists:lookup(dt,Current),
	{_,Speed}=proplists:lookup(sp,Current),
	Settings=maps:get(settings,HState,[]),
	Stop=Speed < proplists:get_value(minspeed,Settings,1),
	%lager:info("stop ~p",[dict:to_list(State#state.data)]),
	{LStart,LStop,LState,LProcessed} = case PI_Data of
										   {A,B,C,D}  -> 
											   {A,B,C,D};
										   {_,{A,B,C,D}} -> 
											   {A,B,C,D};
										   _ -> 
											   case Stop of
												   true -> {T, T, stop, false};
												   _ -> {T, T, drive, false}
											   end
									   end,
	POIs=proplists:get_value(current_poi,maps:get(pi_poi,HState,[]),[]),
	ID=maps:get(id,HState),
	NSSData=case {Stop == (LState == stop), Stop, LProcessed} of
				{true, true, _} -> %Still stopped
					case LProcessed of
						true -> 
							lager:info("Car ~p still stopped for ~p, stopped ~p",[ID,T-LStop,LStop]),
							{LStart, LStop, LState, LProcessed};
						false ->
							lager:info("Stop ~p",[PI_Data]),
							MStop=proplists:get_value(minstop,Settings,300),
							lager:info("Car ~p stopped for ~p, real stop ~p",[ID,T-LStop,MStop]),
							case T - LStop >= MStop of
								true -> 
									savestop(ID,LStop,LStart,T,stop, {Lon, Lat, POIs}),
									lager:info("Car ~p Really stopped at ~p",[ID, LStop]),
									{LStart, LStop, LState, true};
								false -> 
									{LStart, LStop, LState, LProcessed}
							end
					end;
				{true, false, _} -> %Still driveing, do nothing
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
					% lager:info("Car ~p Stopped for a ~p, stopped ~p",[State#state.id,T-LStop,LStop]),
					lager:debug("Car ~p reStart ~p",[ID,T-LStop]),
					{LStart, LStop, drive, false}
			end,
	%lager:info("Car ~p L ~p PrData ~p",[State#state.id,LState,{LStart,LStop,LState,LProcessed}]),
	%lager:info("Car ~p L ~p SSData ~p",[State#state.id,LState,NSSData]),
	{
	 {Stop, NSSData}, 
	 [
	  {isstop,Stop},
	  {tstart,element(1,NSSData)},
	  {tstop,element(2,NSSData)},
	  {status,element(3,NSSData)},
	  {handled,element(4,NSSData)}
	 ]
	}.

savestop(DeviceID, LStop, LStart, Now, Sta, {Lon, Lat, POIs}) -> 
	lager:debug("Car ~p Key: ~p",[DeviceID, {type,events, device,DeviceID, hour,gpstools:floor(LStop/3600)}]),
	StopH=gpstools:floor(LStop/3600),
	KeyS={type,events, device,DeviceID, hour,StopH},
	SKey = list_to_binary("stop."++integer_to_list(LStop)),
	case Sta of
		stop ->
			StartH=gpstools:floor(LStart/3600),
			RKey = list_to_binary("drive."++integer_to_list(LStart)),
			case StartH == StopH of
				true ->
					%lager:info("Combine update1"),
					mng:ins_update(mongo,<<"events">>, KeyS, {
												   SKey,{
													 duration, Now-LStop, 
													 fin, 0, 
													 position, [Lon, Lat], 
													 poi, POIs},
												   RKey,{
													 duration, Now-LStart, 
													 fin, 1, 
													 eposition, [Lon, Lat]
													}
												  });
				_ -> 
					%lager:info("Split update1"),
					KeyR={type,events, device,DeviceID, hour,StartH},
					mng:ins_update(mongo,<<"events">>, KeyS, {SKey,{
																duration, Now-LStop, 
																fin, 0, 
																position, [Lon, Lat], 
																poi, POIs
															   }}),
					mng:ins_update(mongo,<<"events">>, KeyR, {RKey,{
																duration, Now-LStart, 
																fin, 1, 
																eposition, [Lon, Lat]
															   }})
			end;
		drive ->
			NowH=gpstools:floor(Now/3600),
			RKey = list_to_binary("drive."++integer_to_list(Now)),
			case NowH == StopH of
				true -> 
					%lager:info("Combine update2"),
					mng:ins_update(mongo,<<"events">>, KeyS, {
												   <<SKey/binary,".duration">>, Now-LStop,
												   <<SKey/binary,".fin">>, 1,
												   RKey,{
													 duration, Now-LStart, 
													 fin, 0, 
													 position, [Lon, Lat]
													}
												  });
				_ ->
					%lager:info("Split update2"),
					KeyR={type,events, device,DeviceID, hour, NowH},
					mng:ins_update(mongo,<<"events">>, KeyS, {
														 <<SKey/binary,".duration">>, Now-LStop, 
														 <<SKey/binary,".fin">>, 1
														}),
					mng:ins_update(mongo,<<"events">>, KeyR, {RKey,{
																duration, Now-LStart, 
																fin, 0, 
																position, [Lon, Lat]
															   }})
			end;
		_ ->
			ok
	end.


