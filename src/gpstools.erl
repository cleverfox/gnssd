-module(gpstools).

-export([calc_diff/2,floor/1,ceil/1,mod/2,spher_to_cart/1,cart_to_spher/1,rotate/3,sphere_direct/3,sphere_directr/3,sphere_inverse/2,azimuth/2,dist/2,decode_line/2,xmerlval/1,decode_path/1,mkpath/2,get_path/2]).

calc_diff({Lat1, Lng1}, {Lat2, Lng2}) ->
	Deg2rad = fun(Deg) -> math:pi()*Deg/180 end,
	[RLng1, RLat1, RLng2, RLat2] = [Deg2rad(Deg) || Deg <- [Lng1, Lat1, Lng2, Lat2]],

	DLon = RLng2 - RLng1,
	DLat = RLat2 - RLat1,

	A = 
	math:pow(math:sin(DLat/2), 2) + 
	math:cos(RLat1) * 
	math:cos(RLat2) * 
	math:pow(math:sin(DLon/2), 2),

	C = 2 * math:asin(math:sqrt(A)),

	%% suppose radius of Earth is 6372.8 km
	6372.8 * C.

floor(X) ->
	T = erlang:trunc(X),
	case (X - T) of
		Neg when Neg < 0 -> T - 1;
		Pos when Pos > 0 -> T;
		_ -> T
	end.

ceil(X) ->
	T = erlang:trunc(X),
	case (X - T) of
		Neg when Neg < 0 -> T;
		Pos when Pos > 0 -> T+1;
		_ -> T
	end.

mod(X,Y) ->
	X-(floor(X/Y)*Y).


spher_to_cart({Y1,Y0}) ->
  P = math:cos(Y0),
  X2 = math:sin(Y0),
  X1 = P * math:sin(Y1),
  X0 = P * math:cos(Y1),
  {X0,X1,X2}.

hypot(X1,X2) ->
	math:sqrt(math:pow(X1,2)+math:pow(X2,2)).

cart_to_spher({X0,X1,X2}) -> %return vector, vlen
  P = hypot(X0, X1),
  Y1 = math:atan2(X1, X0),
  Y0 = math:atan2(X2, P),
  %{{Y1,Y0},hypot(P, X2)}.
  {Y1,Y0}.

rotate({X0,X1,X2}, A, I) ->
	C = math:cos(A),
	S = math:sin(A),
	J = mod(I+1,3),
	K = mod(I-1,3),
	XJ = case J of 0 -> X0; 1 -> X1; 2 -> X2 end,
	XK = case K of 0 -> X0; 1 -> X1; 2 -> X2 end,
	OXJ = XJ * C + XK * S,
	OXK = -XJ * S + XK * C,
	{
	 case {J, K} of {0, _} -> OXJ; {_, 0} -> OXK; _ -> X0 end,
	 case {J, K} of {1, _} -> OXJ; {_, 1} -> OXK; _ -> X1 end,
	 case {J, K} of {2, _} -> OXJ; {_, 2} -> OXK; _ -> X2 end
	}.

azimuth(S,D) ->
	{AZ,_Dist} = sphere_inverse(S,D),
	AZ.

dist(S,D) ->
	{_AZ,Dist} = sphere_inverse(S,D),
	Dist.

sphere_inverse({S1,S2},{D1,D2}) ->
	{O1,O2}=sphere_inverser(
		  {S1*math:pi()/180,S2*math:pi()/180},
		  {D1*math:pi()/180,D2*math:pi()/180}
			      ),
	{O1*180/math:pi(),O2*6372.8}.

sphere_inverser({P1X,P1Y}, P2) ->
  X=spher_to_cart(P2),
  X1=rotate(X, P1X, 2),
  X2=rotate(X1, math:pi()/2 - P1Y, 1),
  {PT1,PT0}=cart_to_spher(X2),
  {math:pi() - PT1, math:pi()/2 - PT0}.


sphere_direct({S1,S2},Azi, Dist) ->
	{O1,O2}=sphere_directr({S1*math:pi()/180,S2*math:pi()/180},
			       Azi*math:pi()/180,
			       (Dist/6372.8)
			      ),
	{O1*180/math:pi(),O2*180/math:pi()}.

sphere_directr({S1,S2},Azi, Dist) ->
   %double pt[2], x[3];
  PT0 = math:pi()/2 - Dist,
  PT1 = math:pi() - Azi,
  X=spher_to_cart({PT1,PT0}),		% сферические -> декартовы
  X2=rotate(X, S2 - math:pi()/2, 1),	% первое вращение
  X3=rotate(X2, -S1, 2),		% второе вращение
  cart_to_spher(X3).	        	% декартовы -> сферические
 
xsplit([],L1,L2) ->
	case L1 of
		[] -> L2;
		_ -> L2 ++ L1
	end;

xsplit([A|Rest],L1,L2) ->
	case A>=32 of
		true -> 	
			xsplit(Rest,L1 ++ [A-32],L2);
		false ->
			xsplit(Rest,[],L2 ++ [L1 ++ [A]])
	end.

decode_one([],Cur,_Off) -> 
	case (Cur band 1) of
		1 -> (bnot Cur) bsr 1;
		0 -> Cur bsr 1
	end;

decode_one([A|Rest],Cur,Off)->
	decode_one(Rest,Cur bor (A bsl Off),Off+5).

decode_one(List) ->
	decode_one(List, 0, 0).

decode_line(Line,Prec0) ->
	Prec=math:pow(10,-Prec0),
	D1=[ M-63 || M<-Line ],
	D2=[ decode_one(X)*Prec || X <- xsplit(D1,[],[])],
	{D2}.

mkpath({X1,Y1},{X2,Y2})->
	Url="http://www.yournavigation.org/api/dev/route.php"++
"?flat=" ++ float_to_list(Y1,[{decimals, 4}, compact]) ++ 
"&flon=" ++ float_to_list(X1,[{decimals, 4}, compact]) ++ 
"&tlat=" ++ float_to_list(Y2,[{decimals, 4}, compact]) ++ 
"&tlon=" ++ float_to_list(X2,[{decimals, 4}, compact]), 
	lager:info("Requesting ~p",[Url]),
	{ok, {_,_,Body}} = httpc:request(get, {Url, []}, [], []),
	decode_path(Body).

decode_path(Body) ->
	{Xml, _} = xmerl_scan:string(Body),
	{_,C}=xmerlval(xmerl_xpath:string("//coordinates",Xml)),
	B=list_to_binary(C),
	B1=re:replace(B, "^\\s+|\\s+$", "", [{return, binary}, global]),
	Fu=fun(E) -> 
			   case E of 
				   <<"">> ->
					   {0,0};
				   M when is_binary(M) -> 
					   [X,Y]= esub:split(M,","),
					   {binary_to_float(X),binary_to_float(Y)}
			   end
	   end,
	[ Fu(E) || E<-esub:split(B1,"\n")].

get_path(A,B) ->
	Path=case psql:equery("select path from carpath where src=$1 and dst=$2",[A,B]) of
		    {ok,_,Dat} ->
			    [ X || {X} <- Dat ];
		    _Any -> 
				 []
	    end,
	case Path of 
		[] ->
			LL=case psql:equery("select lon,lat from waypoints where id =$1",[A]) of
						{ok,_,[Dat1]} ->
							 case psql:equery("select lon,lat from waypoints where id =$1",[B]) of
								 {ok,_,[Dat2]} ->
									 {Dat1,Dat2};
								 _ -> 
									 false
							 end;
						_ -> 
							false
					end,
			case LL of 
				{{X1,Y1},{X2,Y2}} -> 
					Path1=mkpath({X1,Y1},{X2,Y2}),
					JSON=iolist_to_binary(mochijson2:encode([ [NA,NB] || {NA,NB} <- Path1 ])),
					psql:equery("insert into carpath (src, dst, path) values($1,$2,$3)",[A,B,JSON]),
					Path1;
				_ -> 
					false
			end;
		_ -> [ {X,Y} || [X,Y] <- mochijson2:decode(Path) ]
	end.
	

-include_lib("xmerl/include/xmerl.hrl").
xmerlval(X) ->
	[#xmlElement{name = N, content = [#xmlText{value = V}|_]}] = X,
	{N, V}. 


