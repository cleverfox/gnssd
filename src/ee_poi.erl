-module(ee_poi).
-behaviour(ee).

-include("include/usersub.hrl").

-export([emit/3]).

emit(Sub, HState, Current) ->
	CarID=maps:get(id,HState),
	PI_Params=Sub#usersub.params,
	%lager:info("-----[ ~p ]-----",[?MODULE]),
	%lager:info("I'm ~p:emit(~p,~n~p,~n~p)",[?MODULE, Sub, HState, Current]),
	case proplists:get_value("pois_nodes",PI_Params) of
		LVar when is_list(LVar) ->
			Time=proplists:get_value(dt,Current,0),
			POIs=gb_sets:from_list(LVar),
			CurPOI=maps:get(pi_poi,HState,[]),
			POI_Out=gb_sets:from_list(proplists:get_value(out_poi,CurPOI,[])),
			POI_In=gb_sets:from_list(proplists:get_value(in_poi,CurPOI,[])),
			%lager:info("CurPOI ~p",[POIs]),
			%lager:info("POI_Out ~p",[POI_Out]),
			%lager:info("POI_In ~p",[POI_In]),
			POIExit=gb_sets:to_list(
					  gb_sets:intersection(
						POI_Out, POIs)
					 ),
			POIEnter=gb_sets:to_list(
					   gb_sets:intersection(
						 POI_In, POIs)
					  ),
			[ ee:emit_event(CarID,Sub,Time,?MODULE,enter,[{poi_id,P}]) || P <- POIEnter ],
			[ ee:emit_event(CarID,Sub,Time,?MODULE,exit,[{poi_id,P}]) || P <- POIExit ],
			%lager:info("I am ~p ~p CP ~p",[?MODULE, POIs, CurPOI]),
			lager:info("~p Exit ~p",[?MODULE, POIExit]),
			lager:info("~p Enter ~p",[?MODULE, POIEnter]),
			ok;

		_ -> ok
	end.

