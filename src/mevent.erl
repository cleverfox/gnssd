-module(mevent).

-export([saveevent/3]).

saveevent(DeviceID, EventDescription, Now) -> 
	Key={type,events, device,DeviceID, hour,gpstools:floor(Now/3600)},
	lager:debug("Car ~p Event: ~p, ED ~p",[DeviceID, Key, EventDescription]),
	mng:ins_update(mongo,<<"events">>, Key, EventDescription).

