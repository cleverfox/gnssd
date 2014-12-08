%%%-------------------------------------------------------------------
%%% @author Vladimir Goncharov <devel@viruzzz.org>
%%% @copyright (C) 2014, Vladimir Goncharov
%%% @doc
%%%
%%% @end
%%% Created :  16 Nov 2014 by Vladimir Goncharov
%%%-------------------------------------------------------------------
-module(gnssd).
-author("Vladimir Goncharov").
-behaviour(application).

%% Application callbacks
-include("deps/mongodb/include/mongo_protocol.hrl").

-export([start/0, start/2, stop/1, init/1, test/0]).

-define(MAX_RESTART,    5).
-define(MAX_TIME,      60).

%%%===================================================================
%%% Application callbacks
%%%===================================================================



start() ->
	application:ensure_all_started(gnssd).

start(_StartType, _StartArgs) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
	ok.

init([]) ->
	{MHostname, MPort, MDatabase} = case application:get_env(gnssd,mongodb) of
						{ok, {MHost, Prt, Db}} -> 
							{MHost, Prt, Db};
						_ -> 
							{"localhost",27017,"test"}
					end,
	{RedisHost,RedisPort} = case application:get_env(gnssd,redis) of 
					{ok, {RHost, RPort} } ->
						{RHost,RPort};
					_ ->
						{"127.0.0.1",6379}
				end,
	{SubChan,StripChan} = case application:get_env(gnssd,redis_subscribe) of 
					{ok, {XSub, XStrip} } ->
						{XSub, XStrip};
					_ ->
						{<<"push:*">>,<<"push:">>}
				end,

	PS = case application:get_env(gnssd,pushstream) of 
		     {ok, {Url} } ->
			     Url;
		     _ ->
			     "http://localhost:8080/publish?chan="
	     end,


	{ok,
	 {_SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME},
	  [
%	   { er, 
%	     {er_pool,start_link, [redis, RedisHost, RedisPort, 10]},             % StartFun = {M, F, A}
%	     permanent,                               % Restart  = permanent | transient | temporary
%	     2000,                                    % Shutdown = brutal_kill | int() >= 0 | infinity
%	     worker,                                  % Type     = worker | supervisor
%	     []				        % Modules  = [Module] | dynamic
%	   },
	   {   pool_redis,
	       {poolboy,start_link,[
				    [{name,{local,redis}},
				     {worker_module,eredis},
				     {size,3},
				     {max_overflow,20}
				    ],
				    [ {host, RedisHost}, 
				      {port, RedisPort}
				    ] 
				   ]},            % StartFun = {M, F, A}
	       permanent,                         % Restart  = permanent | transient | temporary
	       5000,                              % Shutdown = brutal_kill | int() >= 0 | infinity
	       worker,                            % Type     = worker | supervisor
	       [poolboy,eredis]                % Modules  = [Module] | dynamic
	   },
	   {   pool_redis1,
	       {poolboy,start_link,[
				    [{name,{local,redis1}},
				     {worker_module,eredis},
				     {size,2},
				     {max_overflow,20}
				    ],
				    [ {host, RedisHost}, 
				      {port, RedisPort},
				      {database, 1}
				    ] 
				   ]},            % StartFun = {M, F, A}
	       permanent,                         % Restart  = permanent | transient | temporary
	       5000,                              % Shutdown = brutal_kill | int() >= 0 | infinity
	       worker,                            % Type     = worker | supervisor
	       [poolboy,eredis]                % Modules  = [Module] | dynamic
	   },
	   {   pool_mongo,
	       {poolboy,start_link,[
				    [{name,{local,mongo}},
				     {worker_module,mc_worker},
				     {size,3},
				     {max_overflow,20}
				    ],
				    [
				     {MHostname,MPort,#conn_state{database=MDatabase}},
				     [
				      {database, MDatabase}
				     ]
				    ]
				   ]},            % StartFun = {M, F, A}
	       permanent,                         % Restart  = permanent | transient | temporary
	       5000,                              % Shutdown = brutal_kill | int() >= 0 | infinity
	       worker,                            % Type     = worker | supervisor
	       [poolboy,mc_worker]                % Modules  = [Module] | dynamic
	   },
	   %	   {   pool_mongo2,
	   %	       {poolboy,start_link,[
	   %				    [{name,{local,mongo2}},
	   %				     {worker_module,mongo_worker},
	   %				     {size,5},
	   %				     {max_overflow,20}
	   %				    ]
	   %				   ]},             % StartFun = {M, F, A}
	   %	       permanent,                               % Restart  = permanent | transient | temporary
	   %	       5000,                                    % Shutdown = brutal_kill | int() >= 0 | infinity
	   %	       worker,                                  % Type     = worker | supervisor
	   %	       [poolboy]                            % Modules  = [Module] | dynamic
	   %	   },
	   {   pool_postgres,
	       {poolboy,start_link,[
				    [{name,{local,postgres}},
				     {worker_module,pgsql_worker},
				     {size,5},
				     {max_overflow,20}
				    ]
				   ]},             % StartFun = {M, F, A}
	       permanent, 5000, worker,
	       [poolboy]
	   },
	   {   redis2nginx,                             
	       {redis2nginx,start_link, [ RedisHost,RedisPort, {SubChan,StripChan}, PS ] },             
	       permanent, 2000, worker,
	       [poolboy,pgsql_worker,epgsql]
	   },
	   {   redissource,
	       {redissource,start_link, [ RedisHost,RedisPort, <<"source">> ] }, 
	       permanent, 2000, worker,
	       [poolboy,pgsql_worker,epgsql]
	   },
	   {   esub,
	       {esub,start_link, [ RedisHost,RedisPort, <<"esub:*">> ] }, 
	       permanent, 2000, worker,
	       [poolboy,pgsql_worker,epgsql]
	   },
	   {   device_sup,
	       {dev_sup,start_link, [ ] },
	       permanent, 2000, supervisor, []
	   }
	   %       {   radius_dispatcher,                             % Id       = internal id
	   %           {radius_dispatcher,start,[]},             % StartFun = {M, F, A}
	   %           permanent,                               % Restart  = permanent | transient | temporary
	   %           2000,                                    % Shutdown = brutal_kill | int() >= 0 | infinity
	   %           worker,                                  % Type     = worker | supervisor
	   %           []        % Modules  = [Module] | dynamic
	   %       },
	   %       {   ippool,                             % Id       = internal id
	   %           {ippool,start,[]},                % StartFun = {M, F, A}
	   %           permanent,                               % Restart  = permanent | transient | temporary
	   %           2000,                                    % Shutdown = brutal_kill | int() >= 0 | infinity
	   %           worker,                                  % Type     = worker | supervisor
	   %           []        % Modules  = [Module] | dynamic
	   %       },
	   %       {    sessions_master,
	   %            {sessions_master, start, []},
	   %            permanent,
	   %            10000,
	   %            worker,
	   %            []
	   %       }
	  ]
	 }
	};

init([Module]) ->
	{ok,
	 {_SupFlags = {simple_one_for_one, ?MAX_RESTART, ?MAX_TIME},
	  [
	   % TCP Client
	   {   undefined,                               % Id       = internal id
	       {Module,start_link,[]},                  % StartFun = {M, F, A}
	       temporary,                               % Restart  = permanent | transient | temporary
	       2000,                                    % Shutdown = brutal_kill | int() >= 0 | infinity
	       worker,                                  % Type     = worker | supervisor
	       []                                       % Modules  = [Module] | dynamic
	   }
	  ]
	 }
	}.

test() ->
	er:lpush(redis,test,"Ololoshki"),
	er:lpush(redis,test,"Ololoshki1"),
	er:lpush(redis,test,"Ololoshki2"),
	io:format("Pop: ~p~n",[er:rpop(redis,test)]),
	io:format("Pop: ~p~n",[er:rpop(redis,test)]),
	io:format("Pop: ~p~n",[er:rpop(redis,test)]).
