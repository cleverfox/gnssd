-module(httprpc_curpath).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

init(Req, State) ->
	Dev=binary_to_integer(cowboy_req:binding(device,Req)),
	lager:info("Curpath ~p",[Dev]),
	{P,R,B,E}=case catch whereis(list_to_existing_atom("device_"++integer_to_list(Dev))) of
				  Pid when is_pid(Pid) ->
					  Points0=gen_server:call(Pid,get_path),
					  CH=time_compat:erlang_system_time(seconds) div 3600,
					  T1=CH*3600,
					  T2=(CH+1)*3600,
					  Points=lists:filter(fun([DT,_]) when DT>=T1 andalso DT<T2 -> true;
									  (_) -> false
										  end, Points0),
					  case Points of
						  [] ->
							  {[],ok,0,0};
						  [_|_] ->
							  [P1,_]=hd(Points),
							  [P2,_]=lists:last(Points),
							  {Points,ok,P1,P2}
					  end;
				  _ -> 
					  {[],noproc,0,0}
			  end,

	Body=jsx:encode(#{
		   result=>R,
		   device_id=>Dev,
		   start=>B,
		   stop=>E,
		   points=>P,
		   msg=><<"">>
		  }),
	Req2 = cowboy_req:reply(200, [
								  {<<"content-type">>, <<"application/json">>}
								 ], Body, Req),

    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.

