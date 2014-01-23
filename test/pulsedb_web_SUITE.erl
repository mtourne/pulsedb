-module(pulsedb_web_SUITE).
-compile(export_all).

all() ->
  [{group, web}].


groups() ->
  [{web, [parallel], [
    embed_error

  ]}].




init_per_suite(Config) ->
  Apps = [crypto, asn1, public_key, ssl, ranch, cowlib, cowboy, lhttpc, pulsedb],
  [application:start(App) || App <- Apps],

  DbSpec = {simple_db, {pulsedb, open, [test_db, [{url,"file://test_db"}]]}, permanent, 100, worker, []},
  {ok, _} = supervisor:start_child(pulsedb_sup, DbSpec),

  Dispatch = [{'_', [
    {"/test_push1", pulsedb_netpush_handler, [{db,test_db},{auth,?MODULE,auth_test1}]},
    {"/test_embed1/[...]", pulsedb_graphic_handler, [{db,test_db},{embed,?MODULE,embed_test1}]},
    {"/test_embed2/[...]", pulsedb_graphic_handler, [{db,test_db},{embed,?MODULE,embed_test2}]}
  ]}],

  ranch:start_listener(test_pulsedb, 1, ranch_tcp, [{port,5674}], cowboy_protocol, [{env, [
    {dispatch, cowboy_router:compile(Dispatch)}
  ]}]),


  R = {Apps},
  [{r,R}|Config].


end_per_suite(Config) ->
  {Apps} = proplists:get_value(r, Config),
  [application:stop(App) || App <- lists:reverse(Apps)],
  Config.




embed_error(_) ->
  {ok, {{500,_}, _, Text}} = lhttpc:request("http://localhost:5674/test_embed1/broken_embed", get, [], 100),
  {match, _} = re:run(Text, "Error rendering this graphic"),
  ok.


