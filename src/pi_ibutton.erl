-module(pi_ibutton).
-export([ds_process/4,ds_process/5,separate/0]).
-include("include/device.hrl").

-export([fetch_keyinfo/1]).
separate() -> 0.

ds_process(PI_Data, Current, Hist, HState) ->  %{private permanent data, public temporary data proplist}
	ds_process(PI_Data, Current, Hist, HState, []).

ds_process(PI_Data0, Current, _Hist, HState, _PI_Params) ->  %{private permanent data, public temporary data proplist}
	PID1=if is_map(PI_Data0) ->
				   PI_Data0;
			   true -> #{}
			end,

	Dev=maps:get(id,HState),
%	lager:info("Plugin IButton Cur ~p",[Current]),
%	lager:info("Plugin PIP ~p",[_PI_Params]),
%	lists:foreach(fun(E) ->
%						  lager:info("Plugin H ~p",[E])
%				  end, maps:to_list(HState)),

	Vars=case proplists:lookup(inputs,maps:get(settings,HState,[])) of
			 {inputs, L} ->
				 lists:filtermap(fun(X) ->
										 case X#incfg.type of
											 {ibutton,_,_} -> {true, X#incfg.variable} ;
											 _ -> false
										 end
								 end, L);
			 _ -> []
		 end,
	%lager:info("Dev ~p INCFG ~p",[Dev,Vars]),
	PID2=lists:filtermap(fun(Var) -> 
%						   lager:info("DS ~p = ~p",[Var, lists:keyfind(Var,1,Current)])
						   case lists:keyfind(Var,1,Current) of
							   {Var, 0}  -> 
								   false;
							   {Var, Val} when is_integer(Val)  -> 
								   {true, Val};
							   _ -> 
								   false
						   end

			  end, Vars),
	%lager:info("Dev ~p DS2 ~p",[Dev,PID2]),
	Add=lists:subtract(PID2,maps:get(current,PID1,[])),
%	Rem=lists:subtract(maps:get(current,PID1,[]),PID2),
	CPOI=fun()-> proplists:get_value(current_poi,maps:get(pi_poi,HState,[]),[]) end,

	{Info2,Remove}=lists:partition(fun({SN,Str}) ->
										   case lists:member(SN,PID2) of
											   true ->
												   true;
											   false ->
												   Str1=[{remove, proplists:get_value(dt,Current)},
														 {remove_poi, CPOI()},
														 {remove_pos, proplists:get_value(position,Current)}|Str],
												   NID=mevent:saveibevent(maps:get(id,HState),remove,Str1),
%												   lager:info("NID ~p",[NID]),
												   savegk(NID, <<"remove_pos">>, proplists:get_value(position,Current)),
												   false
										   end
								   end, 
								maps:get(keyinfo,PID1,[])
							   ),
	Info3=lists:filtermap(fun(SN) ->
								  case fetch_keyinfo(SN) of
									  undefined -> 
										  Str=[
											   {insert, proplists:get_value(dt,Current)},
											   {insert_poi, CPOI()},
											   {insert_pos, proplists:get_value(position,Current)},
											   {serialnum, SN}
											  ],
										  case mevent:saveibevent(maps:get(id,HState),add,Str) of
											  Res when is_binary(Res) ->
												  savegk(Res, <<"insert_pos">>, proplists:get_value(position,Current)),
												  {true, {SN,[{bid,Res}|Str]}};
											  _ ->
												  {true, {SN,Str}}
										  end;
									  {SN, KeyID, Kind, Org, Inv} -> 
										  Str=[
											   {serialnum, SN},
											   {ibutton_id, KeyID},
											   {kind, Kind},
											   {inv, Inv},
											   %{Kind, Inv},
											   {insert, proplists:get_value(dt,Current)},
											   {insert_poi, CPOI()},
											   {insert_pos, proplists:get_value(position,Current)},
											   {org_id, Org}
											  ],
										  case mevent:saveibevent(maps:get(id,HState),add,Str) of
											  Res when is_binary(Res) ->
												  savegk(Res, <<"insert_pos">>, proplists:get_value(position,Current)),
												  {true, {SN,[{bid,Res}|Str]}};
											  _ ->
												  {true, {SN,Str}}
										  end
								  end
						  end, Add),

	if length(Info3)>0 ->
		   lager:info("Dev ~p Add ~p",[Dev, Info3]);
	   true -> ok
	end,
	if length(Remove)>0 ->
		   lager:info("Dev ~p Rem ~p",[Dev, Remove]);
	   true -> ok
	end,

	KeyInfo=Info2 ++ Info3,
	%lager:info("Dev ~p keyinfo ~p",[Dev,KeyInfo]),
		
	{PID1#{ 
	   current => PID2,
	   keyinfo => KeyInfo
	  }, KeyInfo}.


fetch_keyinfo(SN) ->
	try 
		{ok,_Hdr,[{KeyID,Kind,Org,Inv}]}=psql:equery("select id,kind,organisation_id,inv_num from ibuttons where serialnum=$1",[SN]),
		{SN,KeyID,Kind,Org,Inv}
	catch _:_ ->
			  undefined
	end.

	
savegk(ID, Ev, Coords) ->
	try
	lager:info("Geocode ~p ~p ~p",[ID, Ev, Coords]),
	Redis=fun(W) -> 
				  JBin=iolist_to_binary(mochijson2:encode(
										  [
										   {id,mng:id2hex(ID)},
										   {collection, <<"ibutton">>},
										   {ev,Ev},
										   {coords,Coords}
										  ])),
				  lager:info("JBIN ~p",[JBin]),
				  eredis:q(W, [ "lpush", <<"geocode">>, JBin ])
		  end,
	poolboy:transaction(redis,Redis)
	catch _:_ ->
			  ok
	end.

