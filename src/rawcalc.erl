-module(rawcalc).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,run_device/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(state, {
		  timer,
		  curjob=[],
		  childs=[],
		  maxchild=3
		 }).

init(_Args) ->
    {ok, #state{
		timer = erlang:send_after(60000, self(), run)
		   }
	}.


handle_call(status, _From, State) ->
    {reply, {
	   length(State#state.childs),
	   length(State#state.curjob)
	  }, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(stop, State) ->
	erlang:cancel_timer(State#state.timer),
	{noreply, State#state{ curjob=[] }};

handle_info(run, State) ->
	erlang:cancel_timer(State#state.timer),
	NewState=case {length(State#state.childs),length(State#state.curjob)} of
				 {0,0} ->
					 lager:info("Run 0,0"),
					 {true, BS}=mng:raw_cmd(mongo, {distinct, <<"rawdata">>, key, <<"imei">>, query, {done,0}}),
					 PL=proplists:get_value(values,mng:m2proplist(BS),[]),
					 case PL of
						 [] ->
							 State#state{
							   timer = erlang:send_after(90000, self(), run)
							  };
						 _ -> 
							 runjobs(State#state{
									   curjob = PL,
									   timer = erlang:send_after(10000, self(), run)
									  })
					 end;
				 _ ->
					 lager:info("Run N,M"),
					 State#state{
					   timer = erlang:send_after(10000, self(), run)
					  }
			 end,
	{noreply, NewState};

handle_info({'DOWN',_,process,PID,_Reason}, State) ->
	%io:format("PID ~p down ~p~n",[PID,Reason]),
	NewChilds=lists:delete(PID,State#state.childs),
	{noreply, runjobs(State#state{childs=NewChilds})};

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

runjobs(State) when length(State#state.childs)>=State#state.maxchild -> 
	State; %no free slots
runjobs(State) when length(State#state.curjob)==0 -> 
	State; %no more job 
runjobs(State) -> %ok
	[Job|Rest] = State#state.curjob,
	PID=erlang:spawn(rawcalc,run_device,[Job]),
	lager:info("Start worker ~p ~p",[Job,PID]),
	monitor(process,PID),
	runjobs(
	State#state{
	  curjob=Rest,
	  childs=State#state.childs++[PID]
	 }).

%	case length(State#state.childs)<State#state.maxchild of
%		false ->
%			State;
%		true ->
%			ok
%	end.

run_device(X) when is_binary(X) ->
	case global:whereis_name(X) of
		undefined ->
			global:register_name(X, self()),
			EL=mng:find(mongo,<<"rawdata">>,{done,0,imei,X},{'_id',0,hour,1,imei,1}),
			Els=getN(EL,100000),
			mc_cursor:close(EL),
			Ls=lists:sort(fun({_,H1,_,I1},{_,H2,_,I2}) ->
								  if 
									  I2 > I1 -> false;
									  H1 > H2 -> false;
									  true -> true
								  end
						  end,Els),
			lager:info("car ~p Total ~w records~n",[X,length(Ls)]),
			lager:info("Start ~p",[X]),
			run_hour(Ls,now(),now(),0,length(Ls)),
			lager:info("Finish ~p",[X]),
			global:unregister_name(X),
			ok;
		_ -> 
			skip_busy
	end.

getN(_,0) ->
	[];

getN(Cursor,N) ->
	case mc_cursor:next(Cursor) of
		{T} -> [T] ++ getN(Cursor,N-1);
		_ -> []
	end.

run_hour([],_,_,_,_) ->
	done;

run_hour([{hour,H,imei,I}|Rest],STime,PT,Done,Total) ->
	{MSec,Sec,_USec}=now(),
	UT=MSec*1000000+Sec,
	NowHour=UT/3600,
	DevID=case psql:equery("select id from devices where imei=$1", [I]) of
			  {ok, _, [{ID}]} -> 
				  ID;
			  _ ->
				  undefined
				  %autocreate
%				  try 
%					  {ok, _, [{Nid}]}=psql:squery("select nextval('devices_id_seq')"),
%					  NewID=if is_integer(Nid) ->
%								   Nid;
%							   is_binary(Nid) ->
%								   binary_to_integer(Nid)
%							end,
%					  {ok,1} = psql:equery("insert into devices(id, imei, title, settings) values($1,$2,$3,'{}');", 
%										   [NewID,I,<<"Autocreated IMEI ",I/binary>>]),
%					  NewID
%				  catch Ec:Ee ->
%							lager:error("Can't create device ~p. Error ~p:~p",
%										[I,Ec,Ee]),
%							undefined
%				  end
		  end,
	if H >= NowHour ->
			lager:info("Do not run recalc for current hour (IMEI ~p)",[I]),
			runall:run_hour(Rest,STime,PT,Done,Total-1);
		is_integer(DevID) ->
		   {true,{n,HCnt}}=mng:raw_cmd(mongo, {count, <<"rawdata">>, query, {device,DevID,hour,H,done,1}}),
		   if HCnt>0 ->
				  ok=mng:delete(mongo,<<"rawdata">>,{device,DevID,hour,H,done,1}),
				  lager:info("Count ~p ~p del~n", [DevID,H]);
			  true -> 
				  HCnt
		   end,
		   lager:info("2run Imei ~s Hour ~w ID ~w",[I,H,DevID]),
		   Res=device:init([DevID,H,sync]),
		   lager:info("Imei ~s Hour ~w ID ~w: ~w ",[I,H,DevID, Res]),
		   Ts=now(),
		   Spent=timer:now_diff(Ts,STime)/1000000,
		   Spent1=timer:now_diff(Ts,PT)/1000000,
		   Est=Spent1*(Total-Done),
		   %Est=(Spent/(Done+1)*Total)-Spent,
		   lager:info("Runtime ~s ~s, ~w of ~w (~w%) done. Speed ~s/s, estimated ~s~n",
					  [
					   format_time(Spent),
					   format_time(Spent1),
					   Done+1,
					   Total,
					   trunc(((Done+1)/Total)*1000)/10, %не могу представить ситуацию, чтобы Total стал равен нулю
					   float_to_list((Done+1)/Spent,[{decimals, 4}, compact]),
					   format_time(round(Est))]),
		   runall:run_hour(Rest,STime,Ts,Done+1,Total);
	   true ->
		   lager:info("Bad IMEI~p",[I]),
		   runall:run_hour(Rest,STime,PT,Done ,Total-1)
	end.

format_time(T) ->
	if T > 3600 ->
		   float_to_list(T/3600,[{decimals, 2}, compact]) ++ " h";
	   T > 180 ->
		   float_to_list(T/60,[{decimals, 1}, compact]) ++ " min";
	   is_float(T) ->
		   float_to_list(T,[{decimals, 1}, compact]) ++ " sec";
	   is_integer(T) ->
		   integer_to_list(T) ++ " sec"
	end.

