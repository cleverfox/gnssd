-module(exporter_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	{ok, {
	   {one_for_one, 5, 10}, 
	   [
		{   tcpserver_ranch,
			{ranch,start_listener, [
									tcpexporter_pool,
									4,
									ranch_tcp,
									[{port, 8087}],
									exporter_tcpsess,
									[]
								   ] },
			permanent, 5000, supervisor, 
			[]
		}
	   ]
	  } }.

