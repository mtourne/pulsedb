#!/usr/bin/env escript
%%
%%! -env ERL_LIBS .. -pa ebin

-mode(compile).

main([]) ->
  io:format("simple_pusher.erl pulse://host:port [key-of-16-bytes tag1=value1:tag2=value2]\n"),
  halt(1);

main([URL|Args]) ->
  application:load(lager),
  application:set_env(lager,crash_log,undefined),
  application:set_env(lager,handlers,[{lager_console_backend,debug}]),
  lager:start(),

  Auth = case Args of
    [] -> [];
    [Key, TagValues] when length(Key) == 16 ->
      Tags = [ list_to_tuple([list_to_binary(T1) || T1 <- string:tokens(T,"=")]) || T <- string:tokens(TagValues, ":")],
      ApiKey = pulsedb_netpush_auth:make_api_key(list_to_binary(Key), Tags),
      [{api_key,ApiKey}]
  end,

  {ok, DB} = pulsedb_netpush:open(list_to_binary(URL), Auth),


  {Now, _} = pulsedb:current_second(),

  T1 = erlang:now(),
  DB1 = push(1, Now - 100, DB),
  T2 = erlang:now(),
  pulsedb_netpush:close(DB1),
  io:format("Pushed 24 hours during ~B us\n", [timer:now_diff(T2,T1)]),
  ok.


push(86400, _, DB) ->
  DB;

push(N, BaseUTC, DB) ->
  UTC = BaseUTC + N,
  Ticks = [{<<"media_output">>, UTC, random:uniform(10000) + 9000, [{<<"point">>,<<"cdbb536b-efa1-4802-a0e4-d8bcbdcd4ed9">>},
    {<<"media">>, <<"ort", (integer_to_binary(I))/binary>>}] } || I <- lists:seq(1,200)],

  {ok, DB1} = pulsedb_netpush:append(Ticks, DB),

  case N rem 100 of
    0 ->
      {ok, DB2} = pulsedb_netpush:sync(DB1),
      push(N+1, BaseUTC, DB2);
    _ ->
      push(N+1, BaseUTC, DB1)
  end.
  

