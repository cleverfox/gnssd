-module(pi_fuel0).

-export([ds_process/5]).

-record(pi_fuel_pdi, {
		  dt,
		  value,
		  prevsd=0,
		  prevsu=0,
		  prevc=0,
		  postsd=0,
		  postsu=0,
		  postc=0,
		  event
		 }).

-record(pi_fuel_pd, {
		  maxdt,
		  data 
		 }).

%process_hist([{T,HEntry}|HRest], Data, TSt, TCu, Var) ->
%	if 
%		T < TSt ->  % too old data
%			process_hist(HRest, Data, TSt, TCu, Var);
%		true ->  %data within timeframe
%			{_,Value}=proplists:lookup(Var,HEntry),
%			D1=Data+[#pi_fuel_pdi{
%						value=Value
%					   }]
%	end,

pds(Ut,H,V) ->
	%Ut1=case Ut of
	%		auto ->
	%			case proplists:lookup(dt,H) of
	%				{dt, Valt} when is_float(Valt) -> Valt;
	%				{dt, Valt} when is_integer(Valt) -> Valt;
	%				_ -> null
	%			end;
	%		X when is_integer(X) ->
	%			X
	%	end,
	case proplists:lookup(V,H) of
		{V, Val} when is_float(Val) -> {Ut,Val};
		{V, Val} when is_integer(Val) -> {Ut,Val};
		_ ->
			{Ut, null}
	end.


ds_process(PI_Data0, Current, Hist, _HState, _PI_Params) ->  
	Averages=180,
	Var=v_fuel,
	{dt,UnixTime}=proplists:lookup(dt,Current),
	{TCur,LCur}=pds(UnixTime,Current,Var),
	STime=UnixTime-round(Averages*3),

	PI_Data=case PI_Data0 of
				_ when is_record(PI_Data0, pi_fuel_pd) -> 
					%lager:info("PI Data ~p",[PI_Data0]),
					PI_Data0#pi_fuel_pd{
					  data=
					  [ Item || Item <- PI_Data0#pi_fuel_pd.data, Item#pi_fuel_pdi.dt >= STime ]
					  ++[#pi_fuel_pdi{ dt=TCur, value=LCur}]
					 };
				_ ->
					lager:info("new PI Data"),
					IHist = lists:filter(
							  fun ({_,V}) -> 
									  if 
										  V == null -> false; 
										  true -> true 
									  end 
							  end, 
							  [ pds(Ut,H,Var) || {Ut,H} <- Hist, Ut>=STime ]),
					IH1=[ #pi_fuel_pdi{ dt=T, value=L } || {T,L} <- IHist],
					#pi_fuel_pd{
					   maxdt=0,
					   data=IH1++[#pi_fuel_pdi{ dt=TCur, value=LCur}]
					  }
			end,
	
	%lager:debug("Cur ~p",[Current]),
	%HTime=UnixTime-Averages,
	%CenterI=lists:foldl(fun
	%					({T,_}, {undefined, _}) ->
	%							{T, abs(HTime-T)};
	%					({T,_}, {Lt, Ld}) ->
	%							CITD=abs(HTime-T),
	%							if Ld>CITD ->
	%								   {T, abs(HTime-T)};
	%							   true ->
	%								   {Lt, Ld}
	%							end
	%					end, {undefined, undefined}, IHist),
	%lager:debug("Prev ~p",[IHist]),
	T1=PI_Data#pi_fuel_pd.data,
	%lager:debug("Prev1    ~p",[T1]),
	T2=traverse(T1,[],[],Averages,a),
	%lager:debug("Prev2    ~p",[T2]),
	T3=traverse(T2,[],[],Averages,b),
	lager:debug("Prev3 ~p ~p",[length(T3),T3]),
	Evs=findevent(T3,undefined,[]),
	lager:debug("event ~p",[Evs]),

	%{private permanent data, public temporary data proplist}
	{PI_Data#pi_fuel_pd{data=T3}, []}.

findevent([],_,Done) ->
	Done;
findevent([T|Rest],Prev,Done) ->
	T1=T#pi_fuel_pdi{},
	findevent(Rest,T,Done++[T1]).


traverse([],Passed,_,_,_) ->
	Passed;
traverse([E1|Rest],[],Acc,AvgT,Dir) ->
	traverse(Rest,[E1],Acc,AvgT,Dir);
traverse([Cur|Rest],[Prev|Passed],Acc,AvgT,Dir) ->
	%lager:debug("~p~n~p ... ~n~p",[length(Acc),Prev,Cur]),
	CurT=Cur#pi_fuel_pdi.dt,
	MinT=CurT-AvgT,
	MaxT=CurT+AvgT,
	DxDt=(Cur#pi_fuel_pdi.value-Prev#pi_fuel_pdi.value) / (CurT-Prev#pi_fuel_pdi.dt),
	Acc1=lists:filter(fun({Xt,_,_}) ->
							  case Dir of
								  a -> Xt>=MinT;
								  b -> MaxT>=Xt
							  end
					  end,Acc)++[{Cur#pi_fuel_pdi.dt,DxDt,Cur#pi_fuel_pdi.value}],
	{RCnt,RSum}=lists:foldl(
				  fun({_,Dxt,_},{Cnt,Sum}) ->
						  %lager:info("Any ~p",[Dxt]),
						  {Cnt+1,Sum+Dxt}
				  end,
				  {0,0},
				  Acc1),
	Cur1=case Dir of
			 a ->
				 Cur#pi_fuel_pdi{
				   prevc=RCnt,
				   prevsd=RSum
				  };
			 b ->
				 Cur#pi_fuel_pdi{
				   postc=RCnt,
				   postsd=RSum
				  }
		 end,

	traverse(Rest,[Cur1,Prev|Passed],Acc1,AvgT,Dir).

