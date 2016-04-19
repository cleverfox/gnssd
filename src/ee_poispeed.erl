-module(ee_poispeed).
-behaviour(ee).

-include("include/usersub.hrl").

-export([emit/4]).

emit(Sub, HState, Current, _Pre) ->
	CarID=maps:get(id,HState),
	%PI_Params=Sub#usersub.params,
	lager:info("-----[ ~p ]-----",[?MODULE]),
	lager:info("I'm ~p:emit(~p,...,...)",[?MODULE, Sub]),
	CurPPS=maps:get(pi_poispeed,HState,[]),
	Time=proplists:get_value(dt,Current,0),
	lists:foreach(fun({POI, SpeedLimit, start, _}) ->
						  ee:emit_event(CarID,Sub,Time,?MODULE,speeding,[{poi_id,POI},{limit, SpeedLimit}]);
					 ({POI, _SpeedLimit, fin, _}) ->
						  ee:emit_event(CarID,Sub,Time,?MODULE,normal,[{poi_id,POI}]);
					 (_) ->
						  ok
				  end, CurPPS),
	lager:info("PPS ~p",[CurPPS]),
	%{POI,Speed,start, #{ max => ... } },
	%{POI,Speed,fin, #{ max => ..., since => ...  } },
	%{POI,Speed, speeding, #{ max => ..., since => ...  } },
	%{POI,Speed,false,#{}},

	%[ ee:emit_event(CarID,Sub,Time,?MODULE,enter,[{poi_id,P}]) || P <- POIEnter ],
	%[ ee:emit_event(CarID,Sub,Time,?MODULE,exit,[{poi_id,P}]) || P <- POIExit ],
	ok.


