-module(httprpc_export_create).
-behaviour(cowboy_http_handler).
%% Cowboy_http_handler callbacks
-export([
    init/2,
    terminate/3
]).

%binary:replace(binary:replace(base64:encode(<< <<(rand:uniform(255)):8/integer>> || _ <- lists:seq(0,32) >>),<<"+">>,<<"_">>),<<"/">>,<<"r">>)

init(Req, State) ->
	Token=cowboy_req:binding(token,Req),
	{ok,_,_}=psql:squery("create table if not exists export_tokens (token varchar primary key not null);"),
	{ok,_,_}=psql:squery("create table if not exists export_subs (id bigserial primary key not null, token varchar, foreign key (token) references export_tokens(token) on update restrict on delete cascade, device_id bigint not null, foreign key(device_id) references devices(id) on update cascade on delete cascade,unique (token, device_id));"),

	Ac=case psql:equery("select token from export_tokens where token=$1",[Token]) of
		{ok,_Hdr,[_]} ->
			   exists;
		{ok,_Hdr,[]} ->
			   case psql:equery("insert into export_tokens (token) values($1);",[Token]) of
				   {ok, 1} ->
					   ok;
				   _ ->
					   error
			   end;
		_ -> error
	end,
	Body=jsx:encode(#{
		   action => <<"create">>,
		   token => Token,
		   status => Ac
		  }),
	Req2 = cowboy_req:reply(200, 
							[
							 {<<"content-type">>, <<"application/json">>}
							], Body, Req),

    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.

