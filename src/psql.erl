-module(psql).

-export([equery/2,squery/1]).

equery(Sql, Args) ->
	Time=now(),
	Res=poolboy:transaction(postgres, fun(Worker) ->
						      gen_server:call(Worker, {equery, Sql, Args}, 10000)
				      end),
	lager:debug("SQL: ~p, Args ~p, took ~p ms",[Sql,Args, timer:now_diff(now(),Time)/1000]),
	Res.

squery(Sql) ->
	Time=now(),
	Res=poolboy:transaction(postgres, fun(Worker) ->
						      gen_server:call(Worker, {squery, Sql}, 10000)
				      end),
	lager:debug("SQL: ~p, took ~p ms",[Sql, timer:now_diff(now(),Time)/1000]),
	Res.
