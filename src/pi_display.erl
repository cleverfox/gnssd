-module(pi_display).
-export([ds_process/4,ds_process/5, separate/0]).

separate() -> 0.

ds_process(PI_Data, Current, Hist, HState) ->  %{private permanent data, public temporary data proplist}
	ds_process(PI_Data, Current, Hist, HState, []).
	
ds_process(PI_Data, Current, Hist, HState, PI_Args) ->  %{private permanent data, public temporary data proplist}
	{_,T}=proplists:lookup(dt,Current),
	%{MSec,Sec,_} = now(),
	%NowH=gpstools:floor((MSec*1000000 + Sec)/3600),
	%UnixHour=gpstools:floor(T/3600),
	%NeedNotify=(
	%  (UnixHour == NowH orelse UnixHour == NowH-1) 
	%  andalso (T > PI_Data)
	% ),

	case PI_Data of
		LT when is_integer(LT) ->
			if T > LT ->
				   ds_process_real(PI_Data, Current, Hist, HState, PI_Args);
			   true ->
				   {PI_Data,[]}
			end;
		_ ->
			ds_process_real(0, Current, Hist, HState, PI_Args)
	end.

ds_process_real(_PI_Data, Current, _Hist, HState, PI_Args) ->  %{private permanent data, public temporary data proplist}
	{_,[Lon, Lat]}=proplists:lookup(position, Current),
	{_,T}=proplists:lookup(dt,Current),
	{_,Speed}=proplists:lookup(sp,Current),
	{_,Dir}=proplists:lookup(dir,Current),

	Sensors=lists:filtermap(fun({K,V}) when is_atom(K) ->
									LK=atom_to_list(K),
									case LK of
										[$v,$_|_] when is_float(V) -> 
											{true,[list_to_binary(LK),float_to_binary(V, [{decimals, 4}, compact])]};
										[$v,$_|_] when is_integer(V) -> 
											{true,[list_to_binary(LK),integer_to_binary(V)]};
										[$v,$_|_] when is_list(V) -> 
											{true,[list_to_binary(LK),list_to_binary(V)]};
										[$v,$_|_] when is_binary(V) -> 
											{true,[list_to_binary(LK),V]};
										_ ->
											false
									end;
							   (_) -> false
							end,Current),
	%lager:info("C ~p",[Sensors]),
	STOP=maps:get(pi_stop,HState,[]),
	POIs=proplists:get_value(current_poi,maps:get(pi_poi,HState,[]),[]),
	Bi=integer_to_binary(maps:get(id,HState)),
	DevH= <<"device:lastpos:",Bi/binary>>,
	DevP= <<"device:cpoi:",Bi/binary>>,
	Redis=fun(W) -> 
				  eredis:q(W, [ "hmset", DevH, 
								"lng", f2b(Lon),
								"lat", f2b(Lat),
								"dir", f2b(Dir),
								"Start", proplists:get_value(tstart,STOP,0),
								"Stop", proplists:get_value(tstop,STOP,0),
								"Status", proplists:get_value(status,STOP,drive), 
								"StatusHandled", proplists:get_value(handled,STOP,false), 
								"spd", case proplists:get_value(isstop,STOP,false) of 
										   true -> 0; 
										   _ -> f2b(Speed) 
									   end,
								"t", T ] ++ lists:flatten(Sensors) ),
				  eredis:q(W, [ "del", DevP ]),
				  eredis:q(W, [ "sadd", DevP ] ++ POIs)
		  end,
	poolboy:transaction(redis,Redis),

	
	%send data to push stream
	SDir=case Dir of
			 _ when Dir>=337.5 orelse  Dir<22.5  -> <<"↑"/utf8>>;
			 _ when Dir>=22.5  andalso Dir<67.5  -> <<"↗"/utf8>>;
			 _ when Dir>=67.5  andalso Dir<112.5 -> <<"→"/utf8>>;
			 _ when Dir>=112.5 andalso Dir<157.5 -> <<"↘"/utf8>>;
			 _ when Dir>=157.5 andalso Dir<202.5 -> <<"↓"/utf8>>;
			 _ when Dir>=202.5 andalso Dir<247.5 -> <<"↙"/utf8>>;
			 _ when Dir>=247.5 andalso Dir<292.5 -> <<"←"/utf8>>;
			 _ when Dir>=292.5 andalso Dir<337.5 -> <<"↖"/utf8>>
		 end,
	Data={struct,[
				  {type,position},
				  {dev,maps:get(id,HState)},
				  {dir,Dir},
				  {sdir,SDir},
				  {spd,Speed},
				  {pois, POIs},
				  {color, case Speed of
							  M when M<2 -> <<"blue">>;
							  M when M<60 -> <<"green">>;
							  _ -> <<"red">>
						  end},
				  {pos,{array,[Lon, Lat]}}
				 ]},
	%lager:info("JS: ~p",[JSData]),
	notifyPos(PI_Args,Data),

	{T,[]}.


f2b(X) when is_integer(X) ->
       	integer_to_binary(X);
f2b(X) when is_float(X) -> 
	float_to_binary(X,[{decimals, 20}, compact]).

notifyPos(Subscribers,Data) ->
	JSData=iolist_to_binary(mochijson2:encode(Data)),
	Fd=fun(E) ->
			   %lager:info("Send notify ~p ~p",[E,JSData]),
			   gen_server:cast(redis2nginx,{push,<<"push:",E/binary>>,JSData})
	   end,
	lists:foreach(Fd,
				  case lists:keyfind(position,1,Subscribers) of
					  {position, L} when is_list(L) -> L;
					  _ -> []
				  end
				 ),
	ok.

