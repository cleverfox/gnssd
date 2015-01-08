-module(pgsql_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-compile([{parse_transform, lager_transform}]).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(_Args) ->
	{Hostname, Database, Username, Password} =
	case application:get_env(pgsql) of
		{ok, {Host, Db, User, Pass}} -> 
			{Host, Db, User, Pass};
		_ -> 
			{"localhost","template1","pgsql",""}
	end,
	lager:debug("Connect to database ~p@~p:~p~n",[Username,Hostname,Database]),
	case  pgsql:connect(Hostname, Username, Password, [ {database, Database} ]) of
		{ok, Conn} ->
			{ok, #state{conn=Conn}};
		{error, X} ->
			lager:error("Can't connect to database ~p@~p:~p: ~p~n",[Username,Hostname,Database,X])
	end.

handle_call({squery, Sql}, _From, #state{conn=Conn}=State) ->
    {reply, pgsql:squery(Conn, Sql), State};
handle_call({equery, Stmt, Params}, _From, #state{conn=Conn}=State) ->
    {reply, pgsql:equery(Conn, Stmt, Params), State};
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


