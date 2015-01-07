-module(mng).
-export([find/2,find/3,find_one/2,find_one/3,insert/2,ins_update/3,index/3,proplist2tom/1,proplist3tom/1,m2proplist/1]).

find_one(Collection, Query) ->
	lager:info("mFind: ~p, Args ~p",[Collection,Query]),
	poolboy:transaction(mongo, 
						fun(Worker) ->
								mongo:find_one(Worker,Collection, Query)
						end).

find_one(Collection, Query, Extra) ->
	lager:info("mFind: ~p, Args ~p, ~p",[Collection,Query,Extra]),
	poolboy:transaction(mongo, 
						fun(Worker) ->
								mongo:find_one(Worker,Collection, Query, Extra)
						end).

find(Collection, Query) ->
	lager:info("mFind: ~p, Args ~p",[Collection,Query]),
	poolboy:transaction(mongo, 
						fun(Worker) ->
								mongo:find(Worker,Collection, Query)
						end).

find(Collection, Query, Extra) ->
	lager:info("mFind: ~p, Args ~p, ~p",[Collection,Query,Extra]),
	poolboy:transaction(mongo, 
						fun(Worker) ->
								mongo:find(Worker,Collection, Query, Extra)
						end).

insert(Collection, Object) -> 
	lager:debug("Insert into mongo(~p): ~p",[Collection,Object]),
	poolboy:transaction(mongo, 
						fun(Worker) ->
								mongo:insert(Worker,Collection, Object)
						end).

%mng:ins_update(<<"test">>,{type,rawdata,device,1,hour,124},{trololo,[3,4,5,6,{a,1,b,2}]}).
ins_update(Collection, Key, Object) ->
	Time=now(),
	Res=poolboy:transaction(mongo, 
						fun(Worker) ->
								case mongo:find_one(Worker,Collection, Key) of
									{} ->
										mongo:insert(Worker,Collection, Key);
										_ ->
										ok
								end,
								Data={'$set', Object},
								mongo:update(Worker,Collection, Key, Data)
						end),
	lager:debug("Mongo(~p) ins_upd ~p took ~p ms",[Collection, Key, timer:now_diff(now(),Time)/1000]),
	Res.


mongo2proplist([], Arr) ->
	Arr;

mongo2proplist([_], Arr) ->
	Arr;

mongo2proplist([A, B | Rest], Arr) ->
	mongo2proplist(Rest,Arr ++ [{A,B}]).

m2proplist(Term) ->
	mongo2proplist(tuple_to_list(Term),[]).

proplist3tom(List) -> 
	Fx=fun({K,V},AccIn) ->
			   AccIn ++ [K,V]
	   end,
	[ list_to_tuple(lists:foldl(Fx,[],Src)) || {_,_,Src} <- List ].

proplist2tom(List) -> 
	Fx=fun({K,V},AccIn) ->
			   AccIn ++ [K,V]
	   end,
	[ list_to_tuple(lists:foldl(Fx,[],Src)) || {_,Src} <- List ].



%mng:index(<<"test">>,{type,1,device,1,hour,1},true).
index(Collection, Fields, Unique) -> 
	Idx=case Unique of 
		true ->
			{key, Fields, unique, true };
			_ ->
			{key, Fields }
		end,

	poolboy:transaction(mongo, 
						fun(Worker) ->
								mongo:ensure_index(Worker, Collection, Idx)
						end).

