-module(ee_fuel).
-behaviour(ee).

-include("include/usersub.hrl").

-export([emit/4]).

emit(Sub, HState, _Current, Prev0) ->
	CarID=maps:get(id,HState),
	PI_Params=Sub#usersub.params,
	EF=maps:get(pi_fuel,HState,[]),
	PrevTime=case Prev0 of
				 {eef,X} ->
					 X;
				 _ -> 0
			 end,
	case EF of
		[] ->
			{eef, PrevTime};
		_ ->
			lager:debug("I'm ~p:emit(~p,~n~p,~n~p)",[?MODULE, Sub, HState, _Current]),
			lager:debug("I'm ~p ~p",[?MODULE, EF]),
			Sliv=case proplists:get_value(mins,PI_Params) of
					 Y when is_integer(Y) -> Y;
					 Y when is_float(Y) -> Y;
					 _ -> 10
				 end,
			Zapr=case proplists:get_value(minf,PI_Params) of
					 Z when is_integer(Z) -> Z;
					 Z when is_float(Z) -> Z;
					 _ -> 10
				 end,
			lager:debug("I'm ~p ~p",[?MODULE, PI_Params]),
			L1=lists:filter(
				 fun({T,A}) -> 
						 if T<PrevTime -> false;
							A>0 andalso A>=Zapr -> true;
							A<0 andalso -A>=Sliv -> true;
							true -> false
						 end
				 end, EF),
			NexT=lists:foldl(fun({T,A},Acc)->
									 if A > 0 ->
											lager:debug("I'm ~p Fill ~p ~p",[Sub#usersub.evid, T, A]),
											ee:emit_event(CarID,Sub,T,?MODULE,fill,[{amount,A}]);
										true -> 
											lager:debug("I'm ~p Drain ~p ~p",[Sub#usersub.evid, T, A]),
											ee:emit_event(CarID,Sub,T,?MODULE,drain,[{amount,-A}])
									 end,

									 if Acc < T -> T;
										true -> Acc
									 end
							 end,PrevTime,L1),


			lager:debug("I'm ~p ~p nex ~p",[ ?MODULE, L1, NexT ]),

			{eef, NexT}
	end.
