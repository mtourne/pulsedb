-module(pulsedb_web_SUITE).
-compile(export_all).

-define(CRYPT_KEY, <<"0123456789abcdef">>).

all() ->
  [{group, web}].


groups() ->
  [{web, [parallel], [
    embed_error,
    embed_auth,
    embed_no_auth,
    embed_not_found
  ]}].




init_per_suite(Config) ->
  Apps = [lager, crypto, asn1, public_key, ssl, ranch, cowlib, cowboy, lhttpc, pulsedb],
  [application:start(App) || App <- Apps],
  lager:start(),
  DB = {undefined, [{url,"file://test_db"}]},

  Dispatch = [{'_', [
    {"/test_push1", pulsedb_netpush_handler, [{db,DB},{auth,?MODULE,auth_test1}]},
    {"/test_embed1/[...]", pulsedb_graphic_handler, [{db,DB},{embed,?MODULE,embed_test1}]},
                     
    
    {"/test_embed_auth/[...]",         pulsedb_graphic_handler, [{db,DB},{auth,pulsedb_netpush_auth,[{key, ?CRYPT_KEY}]}]},
    {"/test_embed_bad_resolver/[...]", pulsedb_graphic_handler, [{db,DB}, {resolver, ?MODULE, bad_resolve}]},
    {"/test_embed_no_auth/[...]",      pulsedb_graphic_handler, [{db,DB}]},
    {"/test_embed_not_found/[...]",    pulsedb_graphic_handler, [{db,DB},{resolver,?MODULE,resolver_404}]}
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


embed_auth(_) ->
  Q = <<"total_output">>,
  {ok, EncryptedQ} = pulsedb_netpush_auth:encrypt(Q, [{key, ?CRYPT_KEY}]),
  {ok, {{200,_}, _, Text}} = lhttpc:request(<<"http://localhost:5674/test_embed1/", EncryptedQ/binary>>, get, [], 100),
  ok.

embed_error(_) ->
  {ok, {{500,_}, _, Text}} = lhttpc:request("http://localhost:5674/test_embed_bad_resolver/broken_embed", get, [], 100),
  {match, _} = re:run(Text, "Error rendering this graphic"),
  ok.

embed_no_auth(_) ->
  Q = "total_output",
  {ok, {{200,_}, _, Text}} = lhttpc:request("http://localhost:5674/test_embed_no_auth/"++Q, get, [], 100),
  {match, _} = re:run(Text, Q).

embed_not_found(_) ->
  {ok, {{404,_}, _, Text}} = lhttpc:request("http://localhost:5674/test_embed_not_found/aabbccdd", get, [], 100),
  {match, _} = re:run(Text, "This embed didn't exists").




resolver_404(_) ->
  {not_found, "sorry"}.
  
