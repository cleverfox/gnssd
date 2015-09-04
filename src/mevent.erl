-module(mevent).

-export([saveevent/3,saveibevent/3]).

saveevent(DeviceID, EventDescription, Now) -> 
	Key={type,events, device,DeviceID, hour,gpstools:floor(Now/3600)},
	lager:debug("Car ~p Event: ~p, ED ~p",[DeviceID, Key, EventDescription]),
	mng:ins_update(mongo,<<"events">>, Key, EventDescription).

saveibevent(DeviceID, add, IBData) ->
	MngObj=mng:proplisttom([{device, DeviceID}|IBData]),
	IRes=mng:insert(mongo,<<"ibutton">>, MngObj),
	PL=mng:m2proplist(IRes),
	lager:info("SaveIB add Event ~p ~p => ~p",[DeviceID, IBData, PL]),
	{ID}=proplists:get_value('_id',PL),
	ID;

saveibevent(DeviceID, remove, IBData) ->
	lager:info("SaveIB rem Event ~p ~p",[DeviceID, IBData]),
	MngObj=mng:proplisttom(lists:keydelete(bid,1,[{device, DeviceID}|IBData])),
	case proplists:lookup(bid, IBData) of
		{bid, ID} ->
			mng:update(mongo,<<"ibutton">>,{'_id',{ID}}, MngObj),
			ID; 
		_ ->
			PL=mng:insert(mongo,<<"ibutton">>, MngObj),
			{MID}=proplists:get_value('_id',PL),
			MID
	end.
