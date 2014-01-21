#!/usr/bin/env escript
%%
%%! -env ERL_LIBS .. -pa ebin

-mode(compile).

main([]) ->
  io:format("simple_webhandler.erl port path_to_database [auth-key-of-16-byte-length]\n"),
  halt(1);

main(Argv) ->
  process_flag(trap_exit,true),
  try main0(Argv)
  catch
    C:E ->
      io:format("error in boot: ~p:~p\n~p\n", [C,E,erlang:get_stacktrace()]),
      halt(2)
  end.

main0([Port_, Path|Args]) ->
  Port = list_to_integer(Port_),

  Auth = case Args of
    [Key|_] when length(Key) == 16 ->
      NetAuth = [{key,iolist_to_binary(Key)}],
      [{auth,pulsedb_netpush_auth,NetAuth}];
    [] ->
      []
  end,

  application:start(crypto),
  application:start(asn1),
  application:start(public_key),
  application:start(ssl),
  application:start(ranch),
  application:start(cowlib),
  application:start(cowboy),

  application:load(lager),
  application:set_env(lager,crash_log,undefined),
  application:set_env(lager,handlers,[{lager_console_backend,debug}]),
  lager:start(),

  {ok, _} = pulsedb:open(simple_db, [{url,"file://"++Path}]),

  Dispatch = [{'_', [
    {"/api/v1/pulse_push", pulsedb_netpush_handler, [{db,simple_db}] ++ Auth}
  ]}],

  {ok, L} = ranch:start_listener(fake_pulsedb, 1, ranch_tcp, [{port,Port}], cowboy_protocol, [{env, [
    {dispatch, cowboy_router:compile(Dispatch)}
  ]}]),
  erlang:monitor(process,L),
  receive
    {'DOWN', _, _, L, _} -> ok
  end,
  ok.
