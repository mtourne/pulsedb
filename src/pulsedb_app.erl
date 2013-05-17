-module(pulsedb_app).
-export([start/2, stop/1]).

start(_, _) ->
  pulsedb_sup:start_link().

stop(_) ->
  pulsedb_sup:stop().
