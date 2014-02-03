-module(pulsedb_netpush_SUITE).
-compile(export_all).


all() ->
  [{group, netpush}].


groups() ->
  [{netpush, [parallel], [
    append
  ]}].



key() -> <<"0123456789abcdef">>.

start_app(App) ->
  case application:start(App) of
    ok -> {ok, App};
    {error,{already_started,App}} -> {ok, App};
    Other -> Other
  end.


init_per_suite(Config) ->
  Apps = [pulsedb,crypto,asn1,public_key,ssl,ranch,cowlib,cowboy],
  [{ok,App} = start_app(App) || App <- Apps],

  R = [{apps,Apps}],


  Port = 6801,
  NetAuth = [{key,key()}],
  Dispatch = [{'_', [
    {"/api/v1/pulse_push", pulsedb_netpush_handler, [{tracker,pulsedb_netpushers},{path,"temp_push"},{auth,pulsedb_netpush_auth,NetAuth}]}
  ]}],
  {ok, L} = ranch:start_listener(fake_pulsedb, 1, ranch_tcp, [{port,Port}], cowboy_protocol, [{env, [
    {dispatch, cowboy_router:compile(Dispatch)}
  ]}]),

  [{r,R}|Config].


end_per_suite(Config) ->
  R = proplists:get_value(r,Config),
  [application:stop(App) || App <- lists:reverse(proplists:get_value(apps,R))],
  Config.



append(_) ->
  Apikey = pulsedb_netpush_auth:make_api_key(key(), [{<<"point">>, <<"p01">>},{<<"account">>, <<"acc01">>}]),
  {ok, DB1} = pulsedb:open(netpush_client, [{url, "pulse://localhost:6801/"},{api_key,Apikey}]),

  pulsedb:append([
    {<<"input">>, 120, 6, [{name, <<"source-net1">>}]},
    {<<"input">>, 120, 2, [{name, <<"source-net2">>}]},
    {<<"input">>, 130, 10, [{name, <<"source-net1">>}]},
    {<<"input">>, 140, 3, [{name, <<"source-net1">>}]}
  ], netpush_client),

  pulsedb:sync(netpush_client),

  {ok, _} = gen_tracker:find(pulsedb_netpushers, <<"acc01">>),

  {ok, _} = file:read_file_info("temp_push/acc01/1970/01/01/data_v4"),
  {error, enoent} = file:read_file_info("temp_push/1970/01/01/data_v4"),
  ok.









