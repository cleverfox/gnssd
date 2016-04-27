-module(ee).
-include("include/usersub.hrl").
-export([emit_event/6,emit_json/5]).
 
%-callback emit(Sub :: record(usersub), HState :: map(), Current :: list(term())) -> 'ok'|tuple('error', Reason :: string()).
-callback emit(Sub :: tuple(), HState :: map(), Current :: list(term()), Prev :: term) -> 'ok'|tuple().
%-callback handle(Event :: atom()) -> NextEvent :: atom().
%-callback sync(Node :: node(), Timeout :: non_neg_integer()) -> 'ok'|tuple('error', Reason :: string()).


%create_event(Sub :: record(usersub), Data :: list(term()) ) ->
emit_event(CarID, Sub, T, _Name, Event, ExtraData) ->
	%lager:info("~p",[Sub]),
	%mevent:saveevent(
	%  maps:get(id,HState),
	%  { list_to_binary("var.low."++atom_to_list(Var)++"."++integer_to_list(UnixTime)), PT },
	%  UnixTime);
	Name=Sub#usersub.ev_name,

	lager:info("Car ~p event ~p(~p, ~p)",[CarID, Name, Event, ExtraData]),
	JSData=iolist_to_binary(mochijson2:encode(
		   [
			{evid,Sub#usersub.evid},
			{event_name,Sub#usersub.ev_name},
			{type,event},
			{dev,CarID},
			{t,T},
			{severity,Sub#usersub.severity},
			{event_action,Event}
		   ] ++ ExtraData)),
	lager:info("**** JSD ~p ~p",[Sub#usersub.user_id,JSData]),

	emit_json(T, JSData, Sub#usersub.user_id, Sub#usersub.severity, Sub#usersub.user_chan),
	

	ok.

emit_json(T, JSON, UserID, UserSev, UChan) ->
	UserIDb=if is_binary(UserID) -> UserID;
			   is_integer(UserID) -> integer_to_binary(UserID)
			end,
	UserSevb=if is_binary(UserSev) -> UserSev;
			   is_integer(UserSev) -> integer_to_binary(UserSev)
			end,
	DevH= <<"user:",UserIDb/binary,":events:",UserSevb/binary,":events">>,
	DevL= <<"user:",UserIDb/binary,":events:",UserSevb/binary,":lastt">>,
	KeepNum=50,

	RedA=fun(W) -> 
				 N=case eredis:q(W, 
								 [ "rpush", DevH, JSON ]) of
					   {ok, Num} when is_binary(Num) -> 
						   binary_to_integer(Num);
					   _ -> KeepNum+1
				   end,
				 if KeepNum < N -> 
						eredis:q(W, [ "ltrim", DevH, -KeepNum, -1 ]);
					true -> ok
				 end,
				 eredis:q(W, [ "expire", DevH, 86400*7 ]),
				 eredis:q(W, [ "set", DevL, T ])
		 end,
	poolboy:transaction(redis,RedA),
	lager:info("Send notify ~p ~p",[UChan,JSON]),
	gen_server:cast(redis2nginx,{push,<<"push:u.",UChan/binary>>,JSON}),
	ok.
