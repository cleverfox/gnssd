-module(redis_set).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Host, Port) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Host, Port], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Host, Port]) ->
	{ok, Pid} = eredis:start_link([
								   {host, Host},
								   {port, Port}
								  ]),
	lager:info("Eredis up ~p: ~p:~p",[Pid,Host,Port]),
	eredis_sub:controlling_process(Pid),
	{ok, #{redis => Pid}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%handle_cast({cmd, []}, State) ->
%    {noreply, State};

%handle_cast({cmd, [Args|Rest]}, State) ->
%	Redis=maps:get(redis,State),
%	Res=eredis:q(Redis, Args),
%	lager:info("Set ~p ~p: ~p",[Redis,Args,Res]),
%	handle_cast({cmd, Rest}, State);

handle_cast({cmd, Args}, State) ->
	Redis=maps:get(redis,State),
	try
		Res=eredis:q(Redis, Args),
		lager:debug("Set ~p: ~p",[Args,Res])
	catch Ec:Ee ->
			  lager:error("Set ~p error: ~p",[Args,{Ec,Ee}])
	end,
	{noreply, State};

handle_cast({mcmd, Args}, State) ->
	Redis=maps:get(redis,State),
	try
		Res=eredis:qp(Redis, Args),
		lager:debug("Set ~p: ~p",[Args,Res])
	catch Ec:Ee ->
			  lager:error("Set ~p error: ~p",[Args,{Ec,Ee}])
	end,
	{noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

