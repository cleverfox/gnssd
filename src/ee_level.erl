-module(ee_level).
-behaviour(ee).

-include("include/usersub.hrl").

-export([emit/4]).

emit(Sub, HState, Current, Prev) ->
	CarID=maps:get(id,HState),
	lager:debug("-----[ ~p ]-----",[?MODULE]),
	lager:debug("I'm ~p:emit(~p,~n   ~p,~n    ~p,~n    ~p)",[?MODULE, Sub#usersub.params, HState, Current, Prev]),
	case proplists:get_value(var,Sub#usersub.params) of
		BVar when is_binary(BVar) ->
			Var=list_to_atom(binary_to_list(BVar)),
			Time=proplists:get_value(dt,Current,0),
			CurVal=proplists:get_value(Var,Current,0),
			Low=proplists:get_value(low,Sub#usersub.params,null),
			High=proplists:get_value(high,Sub#usersub.params,null),
			NewVal=case CurVal of 
					   M when is_integer(M) orelse is_float(M) ->
						   LOK=Low  == null orelse M>= Low,
						   HOK=High == null orelse High >= M,
						   if HOK andalso LOK ->
								  true;
							  true ->
								  false
						   end;
					   _ ->  %undefined, or something other
						   false
				   end,
			lager:info("level ~p: ~p = ~p, lim ~p ~p. ~p -> ~p",[Time, Var, CurVal, Low, High, Prev, NewVal]),
			if NewVal =/= Prev ->
				   ee:emit_event(CarID,Sub,Time,?MODULE,NewVal,[]);
			   true ->
				   ok
			end,

			NewVal;
		_ -> 
			Prev
	end.

