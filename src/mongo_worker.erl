-module(mongo_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-include("deps/mongodb/include/mongo_protocol.hrl").
-compile([{parse_transform, lager_transform}]).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3,squery/1,find/2]).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(_Args) ->
	lager:info("App ~p~n",[application:get_application()]),
	{Hostname, Port, Database} =
	case application:get_env(gnssd,mongodb) of
		{ok, {Host, Prt, Db}} -> 
			{Host, Prt, Db};
		_ -> 
			{"localhost",27017,"nodb"}
	end,
	lager:info("Connect to database ~p:~p:~p~n",[Hostname,Port,Database]),
	case mongo:connect(Hostname, Port, Database) of
		{ok, Conn} ->
			lager:info("Connection to mongo ~p",[Conn]),
			{ok, #state{conn=Conn}};
		{error, X} ->
			lager:error("Can't connect to database ~p:~p:~p: ~p~n",[Hostname,Port,Database,X])
	end.

handle_call({find, Collection, Query}, _From, #state{conn=Conn}=State) ->
	lager:info("Using connection to mongo ~p",[Conn]),
	Res=mongo:find_one(Conn,Collection, Query),
	{reply, Res, State};
handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = pgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

find(Collection, Query) ->
	lager:info("mFind: ~p, Args ~p",[Collection,Query]),
	poolboy:transaction(mongo, 
			    fun(Worker) ->
					    gen_server:call(Worker, {find, Collection, Query}, 10000)
					    %mongo:find(Worker,Collection, Query)
					    %gen_server:call(Worker, #'query'{
						%		       collection=Collection, 
						%		       selector = bson:append(Query,{})
						%		      },10000)
			    end).

squery(Sql) ->
	lager:info("SQL: ~p",[Sql]),
	poolboy:transaction(mongo, fun(Worker) ->
						   gen_server:call(Worker, {squery, Sql}, 10000)
				   end).
