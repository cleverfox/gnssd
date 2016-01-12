-module(exporter_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    exporter_sup:start_link().

stop(_State) ->
	ranch:stop_listener(tcpexporter_pool),
    ok.
