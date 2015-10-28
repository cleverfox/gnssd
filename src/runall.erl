-module(runall).

-export([raw/1,run_hour/5,list/0,autorun/0,raws/1]).

-compile(nowarn_deprecated_function).

getN(_,0) ->
	[];

getN(Cursor,N) ->
	case mc_cursor:next(Cursor) of
		{T} -> [T] ++ getN(Cursor,N-1);
		_ -> []
	end.

raw([]) -> ok;
raw([X|Rest]) ->
	lager:info("Rawl ~p -> ~p",[X,Rest]),
	raw(X),
	raw(Rest);

raw(C) ->
	lager:info("RUN ~p",[C]),
	case C of
		list ->
			EL=mng:find(mongo,<<"rawdata">>,{done,0},{'_id',0,imei,1}),
			Els=lists:foldl(fun({imei,IM},AC) ->
									PC=maps:get(IM,AC,0),
									maps:put(IM,PC+1,AC)	
							end,#{},getN(EL,10000)),
			mc_cursor:close(EL),
			Els;
		X when is_binary(X) ->
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
					{stop, normal, done};
				_ -> 
					{stop, normal, skip_busy}
			end
	end.

raws(A) ->
	{ok, erlang:spawn(runall,raw,[A])}.

%mng:raw_cmd(mongo, {distinct, <<"rawdata">>, key, <<"imei">>, query, {done,0}}).
autorun() ->
%	{ok,Jobs}=file:consult("job.txt"),
%	lists:foreach(fun({N,Jl}) ->
%						  %Child={N, {runall, raws, [Jl]}, temporary, brutal_kill, worker, []},
%						  %Res=supervisor:start_child(gnss_aggr,Child),
%						  Res=runall:raws(Jl),
%						  io:format("child ~p : ~p ~p~n",[N,Res,Jl])
%				  end,
%				  Jobs).
	{true, BS}=mng:raw_cmd(mongo, {distinct, <<"rawdata">>, key, <<"imei">>, query, {done,0}}),
	PL=proplists:get_value(values,mng:m2proplist(BS),[]),
	Lst=dict:to_list( distribute(PL,0,10,dict:new()) ),
	PIDs=lists:map(fun({N,IMEIs}) ->
						  io:format("Start ~p~n",[IMEIs]),
						  {ok, PID}=runall:raws(IMEIs),
						  monitor(process,PID),
						  io:format("child ~p : ~p~n",[N,PID]),
						  PID
				  end,Lst),
	waitworkers(PIDs).

waitworkers([]) ->
	done;

waitworkers(PIDs) ->
	receive {'DOWN',_,process,PID,Reason} ->
				io:format("PID ~p down ~p~n",[PID,Reason]),
				waitworkers(lists:delete(PID,PIDs))
	after 5000 -> 
				io:format("."),
				waitworkers(PIDs)
	end.



distribute([],_,_,Agg) ->
	Agg;
distribute([Cur|List],I,Num,Agg) ->
	Agg1=dict:append_list(I,[Cur],Agg),
	distribute(List,(I+1) rem Num, Num, Agg1).

list() ->
	poolboy:transaction(mongo, 
						fun(Worker) ->
								%mongo:command(Worker, {count, <<"rawdata">>, 'query', {done,0}})
								mongo:command(Worker, {distinct, <<"rawdata">>, imei}),
								mc_cursor:create(Worker, <<"rawdata">>,
												<<"BasicCursor">>, 1000,100)  
								%mongo:command(Worker, 
								%			  {group, <<"rawdata">>, 
								%				key, {imei, 1},
								%				'cond', {done, 0},
								%				reduce, "function(cur, result){result.count++}", 
								%				initial, {count,0}
								%			  })
								%db.rawdata.group({key: {imei: 1}, cond: {done: 0}, reduce: function(cur, result){result.count++}, initial: {count:0} });
								%db.rawdata.group({key: {imei: 1}, cond: {done: 0}, reduce: "function(cur, result){result.count++}", initial: {count:0} });
						end).

run_hour([],_,_,_,_) ->
	done;

run_hour([{hour,H,imei,I}|Rest],STime,PT,Done,Total) ->
	DevID=case psql:equery("select id from devices where imei=$1", [I]) of
			  {ok, _, [{ID}]} -> 
				  ID;
			  _ ->
				  undefined
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
	if is_integer(DevID) ->
		   {true,{n,HCnt}}=mng:raw_cmd(mongo, {count, <<"rawdata">>, query, {device,DevID,hour,H,done,1}}),
		   if HCnt>0 ->
				  ok=mng:delete(mongo,<<"rawdata">>,{device,DevID,hour,H,done,1}),
				  io:format("Count ~p ~p del~n", [DevID,H]);
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
					   trunc(((Done+1)/Total)*1000)/10,
					   float_to_list((Done+1)/Spent,[{decimals, 4}, compact]),
					   format_time(round(Est))]),
		   ok;
		   %runall:run_hour(Rest,STime,Ts,Done+1,Total);
	   true ->
		   lager:info("Bad IMEI~p",[I]),
		   runall:run_hour(Rest,STime,PT,Done+2 ,Total)
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
	




