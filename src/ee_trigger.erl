-module(ee_trigger).
-behaviour(ee).

-include("include/usersub.hrl").

-export([emit/4]).

emit(Sub, HState, _Current, _Pre) ->
	CarID=maps:get(id,HState),
	PI_Params=Sub#usersub.params,
	%lager:info("I'm ~p:emit(~p,~n~p,~n~p)",[?MODULE, Sub, HState, _Current]),
	%lager:info("I'm ~p ~p",[?MODULE, maps:get(pi_trigger,HState)]),
	case proplists:get_value(var,PI_Params) of
		BVar when is_binary(BVar) ->
			XVar=list_to_atom("v_"++binary_to_list(BVar)),
			Thr=proplists:get_value(threshold,PI_Params,undefined),
			XMin=proplists:get_value(min,PI_Params,Thr),
			XMax=proplists:get_value(max,PI_Params,Thr),
			Matched=[ 
					 {Var,Min,Max,CS,CList} || {Var,Min,Max,CS,CList} <- maps:get(pi_trigger,HState), 
											   Var==XVar, Min==XMin, Max==XMax 
					],
			[{XVar,XMin,XMax,_,CList}|_]=Matched,
			case CList of
				[{low,EvT,PrevT}] -> 
					lager:info("I'm ~p ~p/~p/~p",[Sub#usersub.evid, XVar, XMin, XMax]),
					ee:emit_event(CarID,Sub,EvT,?MODULE,low,[{since,PrevT}]);
				[{high,EvT,PrevT}] -> 
					lager:info("I'm ~p ~p/~p/~p",[Sub#usersub.evid, XVar, XMin, XMax]),
					ee:emit_event(CarID,Sub,EvT,?MODULE,high,[{since,PrevT}]);
				_ ->
					ok
			end,
			lager:info("I'm ~p ~p",[?MODULE,CList]);
		_ -> ok
	end,
	ok.


