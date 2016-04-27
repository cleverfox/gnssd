-module(httprpc_mkevent).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

init(Req, State) ->
	Body=case cowboy_req:method(Req) of
			 <<"POST">> -> 
				 Request=case cowboy_req:body(Req) of
							 {ok, Data, _} -> jsx:decode(Data);
							 {more, Data, _} -> jsx:decode(Data);
							 {error, _Reason} -> []
						 end,
				 EvID=proplists:get_value(<<"evid">>, Request),
				 {ok,_,[{
					  EvID,
					  DevID,
					  UserID,
					  ChanID,
					  EventName,
					  Severity
					  }=PG]}=psql:equery("select d.id,d.device_id,u.id,u.personal_channel,e.name,severity from device_events d left join events e on e.id=event_id left join users u on u.id=user_id where d.id=$1",[EvID]),
				 lager:info("EQ ~p",[PG]),
				 lager:info("Request ~p",[Request]),
				 Timestamp=time_compat:os_system_time(seconds),

				 JSON=jsx:encode([
								  {event_name, EventName},
								  {type, event},
								  {dev, DevID},
								  {t, Timestamp},
								  {severity, Severity}
								  | Request ]),
				 ee:emit_json(Timestamp, JSON, UserID, Severity, ChanID),
				 JSON;
			 _AnyMethod -> Example=jsx:encode(#{
							   evid => <<"usersub_id">>,
							   event_action => <<"event_specific_action">>,
							   extra_param1 => <<"value1">>,
							   extra_param2 => <<"value2">>
							}),
						   <<"Example:\n",Example/binary,"\n">>
		 end,

	Req2 = cowboy_req:reply(200, [
								  {<<"content-type">>, <<"application/json">>}
								 ], Body, Req),

	{ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
	ok.

hint2list(X) when is_binary(X) ->
	binary_to_list(X);

hint2list(_) ->
	null.
