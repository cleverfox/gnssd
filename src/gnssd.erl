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

-export([start/0, stop/0, start/2, stop/1, init/1]).

-define(MAX_RESTART,    30).
-define(MAX_TIME,      60).

%%%===================================================================
%%% Application callbacks
%%%===================================================================



start() ->
	application:ensure_all_started(gnssd).

stop() ->
	application:stop(gnssd).

start(_StartType, _StartArgs) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
	ok.

init([]) ->
	MongoWrk = case application:get_env(mongo_workers) of
				   {ok,X4} when is_integer(X4) -> X4;
				   _ -> 10
			   end,
	RedisWrk = case application:get_env(redis_workers) of
				   {ok,X2} when is_integer(X2) -> X2;
				   _ -> 10
			   end,
	PgWrk = case application:get_env(pgsql_workers) of
				   {ok,X1} when is_integer(X1) -> X1;
				   _ -> 10
			   end,
	MongoCfg = case application:get_env(mongodb) of
				   {ok,X3} when is_list(X3) -> X3;
				   _ -> 
					   lager:error("Can't get mongoDB configuration"),
					   []
			   end,
	{RedisHost,RedisPort} = case application:get_env(redis) of 
					{ok, {RHost, RPort} } ->
						{RHost,RPort};
					_ ->
						{"127.0.0.1",6379}
				end,
	{SubChan,StripChan} = case application:get_env(redis_subscribe) of 
					{ok, {XSub, XStrip} } ->
						{XSub, XStrip};
					_ ->
						{<<"push:*">>,<<"push:">>}
				end,

	PS = case application:get_env(pushstream) of 
		     {ok, {Url} } ->
			     Url;
		     _ ->
			     "http://localhost:8080/publish?chan="
	     end,


	{ok,
	 {_SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME},
	  [
	   {   pool_redis,
	       {poolboy,start_link,[
				    [{name,{local,redis}},
				     {worker_module,eredis},
				     {size,RedisWrk},
				     {max_overflow,20}
				    ],
				    [ {host, RedisHost}, 
				      {port, RedisPort}
				    ] 
				   ]}, 
	       permanent, 5000, worker,
	       [poolboy,eredis]
	   },
	   {   pool_mongo,
	       {poolboy,start_link,[
				    [{name,{local,mongo}},
				     {worker_module,mc_worker},
				     {size,MongoWrk},
				     {max_overflow,20}
				    ],
					MongoCfg
				   ]},
	       permanent, 5000, worker, 
		   [poolboy,mc_worker]
	   },
	   {   pool_postgres,
	       {poolboy,start_link,[
				    [{name,{local,postgres}},
				     {worker_module,pgsql_worker},
				     {size,PgWrk},
				     {max_overflow,20}
				    ]
				   ]},
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
	   {   redislsource,
	       {redislsource,start_link, [ RedisHost,RedisPort ] }, 
	       permanent, 2000, worker,
	       [poolboy,pgsql_worker,epgsql]
	   },
	   {   geocoder,
	       {geocoder,start_link, [ RedisHost,RedisPort ] }, 
	       permanent, 2000, worker,
	       []
	   },
	   {   flogger,
	       {flogger,start_link, []}, 
	       permanent, 2000, worker,
	       []
	   },
	   {   rawcalc,
	       {rawcalc,start_link, []}, 
	       permanent, 2000, worker,
	       []
	   },
	   {   erlsource,
	       {erlsource,start_link, [ ] }, 
	       permanent, 2000, worker,
	       [poolboy,pgsql_worker,epgsql]
	   },
	   {   esub,
	       {esub2,start_link, [ RedisHost,RedisPort, <<"esub:*">> ] }, 
	       permanent, 2000, worker,
	       [poolboy,pgsql_worker,epgsql]
	   },
	   {   devnotify,
	       {devnotify,start_link, [ RedisHost,RedisPort, <<"device_changed">> ] }, 
	       permanent, 2000, worker,
	       []
	   },
	   {   device_sup,
	       {dev_sup,start_link, [ ] },
	       permanent, 2000, supervisor, []
	   },
	   {   recalculator_sup,
	       {recalculator_sup,start_link, [ ] },
	       permanent, 2000, supervisor, []
	   },
	   {   recalculator_dispatcher,
	       {recalculator_dispatcher,start_link, [ RedisHost, RedisPort, "recalc" ] },
	       permanent, 2000, worker, []
	   }

	   %
%,
%	   {   generator_sup,
%	       {generator_sup,start_link, [ ] },
%	       permanent, 2000, supervisor, []
%	   }
	  ]
	 }
	}.


