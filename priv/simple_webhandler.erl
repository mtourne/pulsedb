#!/usr/bin/env escript
%%
%%! -env ERL_LIBS ..:deps -pa ebin

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

  application:load(pulsedb),
  application:set_env(pulsedb, port, Port),
  application:set_env(pulsedb, path, Path),
  case Args of
    [Key] -> application:set_env(pulsedb, key, Key);
    [] -> ok
  end,

  {ok, L} = pulsedb_launcher:start(),
  erlang:monitor(process,L),
  receive
    {'DOWN', _, _, L, _} -> ok
  end,
  ok.

