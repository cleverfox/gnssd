-module(pi_trigger).
-export([ds_process/5,separate/0]).

separate() -> true.

ds_process(PI_Data, Current, _Hist, HState, PI_Params) ->  
	{dt,UnixTime}=proplists:lookup(dt,Current),
	case proplists:get_value(var,PI_Params) of
		undefined ->
			{PI_Data,[]};
		BVar ->
			Var=list_to_atom("v_"++binary_to_list(BVar)),
			{PT,PV}=case PI_Data of
				   {low,T} -> {T,low};
				   {high,T} -> {T,high};
				   _ -> {UnixTime,undefined}
			   end,
			CVal=proplists:get_value(Var,Current,0),
			AS=proplists:get_value(autosave,PI_Params,false),
			Thr=proplists:get_value(threshold,PI_Params,undefined),
			Min=proplists:get_value(min,PI_Params,Thr),
			Max=proplists:get_value(max,PI_Params,Thr),
			{New,ST,Ev}=case PV of
						 low when CVal>=Max -> 
								if AS == true ->
									   mevent:saveevent(
										 maps:get(id,HState),
										 { list_to_binary("var.high."++atom_to_list(Var)++"."++integer_to_list(UnixTime)), PT },
										 UnixTime);
								   true -> ok
								end,
							 {high,UnixTime,{high,UnixTime,PT}};
						 high when Min>=CVal -> 
								if AS == true ->
									   mevent:saveevent(
										 maps:get(id,HState),
										 { list_to_binary("var.low."++atom_to_list(Var)++"."++integer_to_list(UnixTime)), PT },
										 UnixTime);
									   true -> ok
								end,
							 {low,UnixTime,{low,UnixTime,PT}};
						 low -> {low,PT,none};
						 high -> {high,PT,none}; 
						 _ -> {low, UnixTime,unknown}
					 end,

			lager:debug("~p Prev ~p Cur ~p Low ~p High ~p New ~p",[Var,PV,CVal,Min,Max,{New,ST}]),
			{
			 {New,ST},
			 [ {Var, Min, Max, New, [Ev] } ]
			}
	end.
