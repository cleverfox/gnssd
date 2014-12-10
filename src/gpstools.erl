-module(gpstools).

-export([calc_diff/2,calc_az/2,azymuth/2,floor/1,mod/2,spher_to_cart/1,cart_to_spher/1,rotate/3,sphere_direct/3,sphere_directr/3]).

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

mod(X,Y) ->
	X-(floor(X/Y)*Y).


calc_az({Lat1, Lng1}, {Lat2, Lng2}) ->
	Deg2rad = fun(Deg) -> math:pi()*Deg/180 end,
	[RLng1, RLat1, RLng2, RLat2] = [Deg2rad(Deg) || Deg <- [Lng1, Lat1, Lng2, Lat2]],
	DLon = RLng2 - RLng1,

	X=(math:cos(RLat1)*math:sin(RLat2)) - (math:sin(RLat1)*math:cos(RLat1)*math:cos(DLon)),
	Y=math:sin(DLon)*math:cos(RLat2),
	Z=math:atan(-Y/X)/math:pi()*180,
	Z1=case (X < 0) of
		   true -> Z+180;
		   false -> Z
	   end,
	Z2=Deg2rad(-(mod(Z1+180,360)-180)),
	PI2=2*math:pi(),
	AR2=Z2-(PI2*floor(Z2/PI2)),
	%% suppose radius of Earth is 6372.8 km
	AR2*180/math:pi().

azymuth({SX,SY},{DX,DY}) ->
	DeltaX=DX-SX,
	DeltaY=DY-SY,
	A=180*((math:atan(DeltaX/DeltaY))/(math:pi())),
	case A > 0 of
		true -> A;
		false -> 360+A
	end.

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


sphere_direct({S1,S2},Azi, Dist) ->
	{O1,O2}=sphere_directr({S1*math:pi()/180,S2*math:pi()/180},
			       Azi*math:pi()/180,
			       Dist/(2*math:pi()*6372.8)),
	{O1*180/math:pi(),O2*180/math:pi()}.
sphere_directr({S1,S2},Azi, Dist) ->
   %double pt[2], x[3];
  PT0 = math:pi()/2 - Dist,
  PT1 = math:pi() - Azi,
  X=spher_to_cart({PT1,PT0}),		% сферические -> декартовы
  X2=rotate(X, S2 - math:pi()/2, 1),	% первое вращение
  X3=rotate(X2, -S1, 2),		% второе вращение
  cart_to_spher(X3).	        	% декартовы -> сферические
 
