-module(psql).

-export([equery/3,squery/2,equery/2,squery/1]).

equery(Sql, Args) ->
    equery(postgres, Sql, Args).
    
equery(Pool, Sql, Args) ->
	Time=now(),
	Res=poolboy:transaction(Pool, fun(Worker) ->
						      gen_server:call(Worker, {equery, Sql, Args}, 10000)
				      end),
	lager:debug("SQL: ~p, Args ~p, took ~p ms",[Sql,Args, timer:now_diff(now(),Time)/1000]),
	Res.

squery(Sql) ->
    squery(postgres, Sql).

squery(Pool,Sql) ->
	Time=now(),
	Res=poolboy:transaction(Pool, fun(Worker) ->
						      gen_server:call(Worker, {squery, Sql}, 10000)
				      end),
	lager:debug("SQL: ~p, took ~p ms",[Sql, timer:now_diff(now(),Time)/1000]),
	Res.
