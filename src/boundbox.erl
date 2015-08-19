-module(boundbox).
-export([ppdist/6]).

ppdist(X1,X2,Y1,Y2,Lon,Lat) when X2>=Lon andalso Lon>=X1  -> %inside
	if Y2>=Lat andalso Lat>=Y1 -> %inside
		   0;
	   Lat>Y2 -> %up
		   geotools:dist_simple({Lon,Y2},{Lon,Lat});
	   Lat<Y1 -> %down
		   geotools:dist_simple({Lon,Y1},{Lon,Lat})
	end;

ppdist(_X1,X2,Y1,Y2,Lon,Lat) when Lon>=X2 -> %right
	if Y2>=Lat andalso Lat>=Y1 -> %middle
		   geotools:dist_simple({X2,Lat},{Lon,Lat});
	   Lat>Y2 -> %up
		   geotools:dist_simple({X2,Y2},{Lon,Lat});
	   Lat<Y1 -> %down
		   geotools:dist_simple({X2,Y1},{Lon,Lat})
	end;

ppdist(X1,_X2,Y1,Y2,Lon,Lat) when X1>=Lon -> %left
	if Y2>=Lat andalso Lat>=Y1 -> %middle
		   geotools:dist_simple({X1,Lat},{Lon,Lat});
	   Lat>Y2 -> %up
		   geotools:dist_simple({X1,Y2},{Lon,Lat});
	   Lat<Y1 -> %down
		   geotools:dist_simple({X1,Y1},{Lon,Lat})
	end;

ppdist(X1,X2,Y1,Y2,Lon,Lat) ->
	throw({badarg,X1,X2,Y1,Y2,Lon,Lat}).

