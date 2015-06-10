-module(econfig).

-export([reload/3,reload/2,reload/1]).

get_config_name() ->
    case init:get_argument(config) of
        error ->
            throw({error,{nodeclt_config,undefined}});
        {ok,[[File]]} ->
            File
    end.


get_app_params(App, File) ->
    case file:consult(File) of
        {ok, [L]} ->
            proplists:get_value(App, L, []);
        Err -> throw(Err)
    end.

reload(App, #{}=Cfg_procs, Notify) ->
    try
        Cfg_file = get_config_name(),
        Args = get_app_params(App, Cfg_file),
        UpdateCfg=fun({W,C}) -> 
                          case application:get_env(App,W) of
                              {ok, C} -> {skip, W};
                              _ -> 
                                  ok=application:set_env(App,W,C),
                                  case Notify of 
                                      X when is_pid(X) ->
                                          X ! {changed, App, W};
                                      _ -> ok
                                  end,
                                  case maps:find(W,Cfg_procs) of
                                      {ok, PID} ->
                                          PID ! reload,
                                          {notified, W};
                                      _ -> 
                                          {updated, W}
                                  end
                          end
                  end,
        lists:map(UpdateCfg, Args)
        %notify_procs(cfg_reloaded, Cfg_procs),
        %after_reload(),
    catch
        _:{error, Why} ->
            lager:error("Reload error ~p",[Why]),
            {error, Why};
        _:Err ->
            lager:error("Reload error ~p",[Err]),
            {error, Err}
    end.

reload(App) ->
    reload(App,#{},undefined).

reload(App,X) when is_pid(X) ->
    reload(App,#{},X);

reload(App,X) when is_map(X) ->
    reload(App,X, undefined).

