-module(flogger).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([log/2]).
-record(state, {
		  sizelimit = 1*1024*1024,
		  files = #{},
		  timer
		 }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

log(Filename,Payload) ->
	gen_server:cast(flogger,{log,Filename,Payload}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
	{ok, 
	 #state{
		timer = erlang:send_after(1000, self(), checksize)
	   }
	}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({log, Filename, Payload}, State) ->
	State2=case maps:get(Filename,State#state.files,undefined) of
			   undefined ->
				   {ok, File}=file:open(Filename,[append]),
				   erlang:group_leader(File, self()),
				   store_payload(File,Payload),
				   State#state{files=maps:put(Filename,File,State#state.files)};
			   File ->
				   store_payload(File,Payload),
				   State
	end,
    {noreply, State2}.


handle_info(checksize, State) ->
	erlang:cancel_timer(State#state.timer),
	Files=lists:filtermap(fun({Filename,IoDev})->
							try
								{ok,Finfo}=file:read_file_info(Filename),
								Size=element(2,Finfo),
								if Size>1024*1024 ->
									   lager:info("Rotating ~p ~p",
										   [Filename, Size]),
									   file:close(IoDev),
									   rotate_file(Filename,10),
									   false;
								   true ->
									   {true,{Filename,IoDev}}
								end
							catch _:_ -> 
									  {true,{Filename,IoDev}}
							end
					end,
					maps:to_list(State#state.files)
				   ),
	{noreply,
	 State#state{
	   files = maps:from_list(Files),
	   timer = erlang:send_after(10000, self(), checksize)
	   }
	};

handle_info(close, State) ->
	erlang:cancel_timer(State#state.timer),
	lists:foreach(fun({Filename,IoDev})->
							  lager:info("Closing ~p", [Filename]),
							  catch file:close(IoDev)
						  end,
						  maps:to_list(State#state.files)
				   ),
	{noreply,
	 State#state{
	   files = #{},
	   timer = erlang:send_after(10000, self(), checksize)
	   }
	};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

store_payload(File, Payload) ->
	{{Yr,Mon,Day},{Hr,Min,Sec}}=calendar:local_time(),
	UT={Yr,Mon,Day,Hr,Min,Sec},
	IOL=if is_binary(Payload) ->
			   io_lib:fwrite("~w ~s~n",[UT,Payload]);
		   true ->
			   io_lib:fwrite("~w ~p~n",[UT,Payload])
		end,
	file:write(File, IOL).

rotate_file(Filename,MaxFiles) ->
	rename_file(Filename,0,MaxFiles).

rename_file(Filename,Number,Max) ->
	NewName=Filename++".p"++integer_to_list(Number+1),
	MyName=case Number of 
			   0 -> Filename;
			   _ -> Filename++".p"++integer_to_list(Number)
		   end,
	case file:read_file_info(NewName) of
		{ok,_} ->
			if Number+1 == Max ->
				   file:delete(NewName);
			   true ->
				   rename_file(Filename,Number+1,Max)
			end,
			file:rename(MyName,NewName);
		{error, enoent} -> 
			file:rename(MyName,NewName)
	end.


