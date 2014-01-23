#!/usr/bin/env escript


main([]) ->
  Path1 = "src/pulsedb.app.src",
  Path2 = "ebin/pulsedb.app",
  case file:consult(Path1) of
    {ok, [{application, pulsedb, Env}]} -> io:format("~s~n", [proplists:get_value(vsn,Env)]);
    {error, enoent} ->
       {ok, [{application, pulsedb, Env}]} = file:consult(Path2),
       io:format("~s~n", [proplists:get_value(vsn,Env)])
  end.

