#!/usr/bin/env ERL_LIBS=.. escript

%%
%%! -env ERL_LIBS .. -pa ebin

-mode(compile).

main([Port_, Path]) ->
  Port = list_to_integer(Port_),

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

  Dispatch = [{'_', [
    {"/api/v1/pulse_push", pulsedb_netpush_handler, [{path,Path}]}
  ]}],
  {ok, L} = ranch:start_listener(fake_pulsedb, 1, ranch_tcp, [{port,Port}], cowboy_protocol, [{env, [
    {dispatch, cowboy_router:compile(Dispatch)}
  ]}]),
  erlang:monitor(process,L),
  receive
    {'DOWN', _, _, L, _} -> ok
  end,
  ok.
