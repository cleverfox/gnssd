-module(httprpc_export).
%% Cowboy_http_handler callbacks
-export([
	routes/0
]).

routes() ->
	[
	 {"/bapi/exportsubs/create/:token", httprpc_export_create, []},
	 {"/bapi/exportsubs/delete/:token", httprpc_export_delete, []},
	 {"/bapi/exportsubs/flush/:token", httprpc_export_flush, []},
	 {"/bapi/exportsubs/subscribe/:token", httprpc_export_subscribe, []},
	 {"/bapi/exportsubs/unsubscribe/:token", httprpc_export_unsubscribe, []},
	 {"/bapi/exportsubs/resend/:token", httprpc_export_resend, []}
	].

