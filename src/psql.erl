-module(psql).

-export([equery/2,squery/1]).

equery(Sql, Args) ->
	lager:info("aSQL: ~p, Args ~p",[Sql,Args]),
	poolboy:transaction(postgres, fun(Worker) ->
						      gen_server:call(Worker, {equery, Sql, Args}, 10000)
				      end).

squery(Sql) ->
	lager:info("SQL: ~p",[Sql]),
	poolboy:transaction(postgres, fun(Worker) ->
						      gen_server:call(Worker, {squery, Sql}, 10000)
				      end).
