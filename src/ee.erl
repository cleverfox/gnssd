-module(ee).
-include("include/usersub.hrl").
-export([emit_event/6]).
 
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
	UserIDb=integer_to_binary(Sub#usersub.user_id),
	UserSev=integer_to_binary(Sub#usersub.severity),
	DevH= <<"user:",UserIDb/binary,":events:",UserSev/binary,":events">>,
	DevL= <<"user:",UserIDb/binary,":events:",UserSev/binary,":lastt">>,
	lager:info("Car ~p event ~p",[CarID, DevH]),
	KeepNum=50,

	JSData=iolist_to_binary(mochijson2:encode(
		   [
			{evid,Sub#usersub.evid},
			{event_name,Sub#usersub.ev_name},
			{type,event},
			{dev,CarID},
			{t,T},
			{severity,UserSev},
			{event_action,Event}
		   ] ++ ExtraData)),
	lager:info("**** JSD ~p ~p",[DevH,JSData]),
	RedA=fun(W) -> 
				 N=case eredis:q(W, 
								 [ "rpush", DevH, JSData ]) of
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
	UChan=Sub#usersub.user_chan,
	lager:info("Send notify ~p ~p",[UChan,JSData]),
	gen_server:cast(redis2nginx,{push,<<"push:u.",UChan/binary>>,JSData}),

	ok.


