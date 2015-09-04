-module(ee_badibutton).
-behaviour(ee).

-include("include/usersub.hrl").

-export([emit/4]).

emit(Sub, HState, Current, Prev0) ->
	Prev=if is_list(Prev0) ->
				Prev0;
			true ->
				[]
		 end,

	CarID=maps:get(id,HState),
	lager:debug("-----[ ~p ]-----",[?MODULE]),
%	lager:info("I'm ~p:emit(~p,~n   ~p)",[?MODULE, Sub#usersub.params, HState]),
	CurIB=maps:get(pi_ibutton,HState,[]),
	CurKeys=lists:map(fun({Key,_}) -> Key end, CurIB),
	if CurKeys == Prev ->
		   Prev;
	   true ->
		   lager:info("Cur IB ~p",[CurIB]),
		   lists:foreach(fun({Key,Data}) -> 
								 case lists:member(Key,Prev) of 
									 true ->
										 ok;
									 false ->
										 lager:info("New Key ~p",[Data]),
										 case proplists:get_value(ibutton_id,Data) of
											 undefined ->
												 Time=proplists:get_value(dt,Current,0),
												 ee:emit_event(CarID,Sub, Time,
															   ?MODULE, 
															   <<"insert">>, 
															   [ { serialnum, Key } ]
															  );
											 _ -> ok
										 end
								 end
						 end, CurIB),
		   CurKeys
	end.

