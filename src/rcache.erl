-module(rcache).
-export([
	cget/3,
	cflush/1
]).

cflush(Key) ->
	poolboy:transaction(redis, fun(W)-> 
									   {ok, L}=eredis:q(W,[ "keys", Key ]),
									   lists:foreach(fun(E) ->
															 eredis:q(W,[ "del", E ])
													 end,L)
							   end).

cget(Key, Fun, Params) ->
	case poolboy:transaction(redis, fun(W)-> eredis:q(W,[ "get", Key ]) end) of
		{ok, undefined} ->
			lager:debug("Key ~p not found",[Key]),
			Res=Fun(Params),
			poolboy:transaction(redis, fun(W)-> eredis:q(W,[ 
															"setex",
															 Key,
															 maps:get(expire, Params, 3600),
															 term_to_binary(Res) 
														   ]) end),
			Res;
		{ok,BTerm} when is_binary(BTerm) -> 
			lager:debug("Key ~p found in cache",[Key]),
			binary_to_term(BTerm)
	end.


