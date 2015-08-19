-module(pi_fuel).

-export([ds_process/5,test/0,separate/0]).

-record(pi_fuel_r, {
		  nextproc=0,
		  needcheck=false,
		  lastaggt=0
		 }).

separate() -> 0.

ds_process(PI_Data0, _Current, _Hist, HState, _PI_Params) ->  
	CarID = maps:get(id,HState),
	CHour = maps:get(chour,HState),
	{MSec,Sec,_}=now(), %TODO fix to dt
	UnixTime=(MSec*1000000+Sec),
	PI_Data=case PI_Data0 of
				_ when is_record(PI_Data0, pi_fuel_r) -> 
					PI_Data0;
				_ ->
					#pi_fuel_r{nextproc=0}
			end,
	if 
		UnixTime > PI_Data#pi_fuel_r.nextproc ->
			lager:debug("PIPP ~p ~p",[CarID, PI_Data]),
			case dict:find(mngid,maps:get(data,HState,dict:new())) of
				{ok, MID} ->
					HID=list_to_binary(mng:id2hex(MID)),
					Redis=fun(W) -> 
								  eredis:q(W, [ "lpush", <<"aggregate:express">>, <<HID/binary,":agg_fuelgauge">>]),
								  eredis:q(W, [ "publish", <<"aggregate">>, HID ])
						  end,
					poolboy:transaction(redis,Redis),

					lager:debug("MID ~p ~p",[CarID, HID]),
					MID;
				_ ->
					ok
			end,

			{PI_Data#pi_fuel_r{nextproc=UnixTime+60,needcheck=true}, []};
		PI_Data#pi_fuel_r.needcheck ->
			DKey="device:fuel:"++integer_to_list(CarID)++":"++integer_to_list(CHour)++":",
			R1=fun(W) -> 
					   {ok,BT}=eredis:q(W, [ "get", DKey++"lastrun" ]),
					   T=case BT of
							 undefined -> 
								 0;
							 _ when is_binary(BT) ->
								 binary_to_integer(BT)
						 end,
					   if T>PI_Data#pi_fuel_r.lastaggt ->
							  {ok,Evs}=eredis:q(W, [ "hgetall", DKey++"events" ]),
							  EvL=redisl2pl(Evs),
							  lager:debug("PIch ~p evs ~p",[CarID, EvL]),
							  {PI_Data#pi_fuel_r{needcheck=false,lastaggt=T},EvL};
						  true ->
							  {PI_Data,[]}
					   end
				  end,
			Rr=poolboy:transaction(redis,R1),
			lager:debug("PIch ~p ~p ~p",[CarID, PI_Data, Rr]),
			Rr;
		true ->
			lager:debug("PI   ~p ~p",[CarID, PI_Data]),
			{PI_Data,[]}
	end.


test() ->
	Source=[<<"1427357276">>,<<"18.9219">>,<<"1427357275">>,<<"18.9218">>],
	[{1427357276,18.9219},{1427357275,18.9218}]=redisl2pl(Source),
	ok.

redisl2pl([_]) ->
	throw('bad_list');
redisl2pl([]) ->
	[];
redisl2pl([A,B|Rest]) ->
	[{binary_to_integer(A),binary_to_float(B)}]++redisl2pl(Rest).
	



