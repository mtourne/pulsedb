-module(pulsedb_sup).
-export([start_link/0, stop/0, init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop() ->
  erlang:exit(erlang:whereis(?MODULE), shutdown).

init([]) ->
  Supervisors = [
    {pulsedb_repeater, {pulsedb_repeater, start_link, []}, permanent, 100, worker, []}
  ],
  {ok, {{one_for_one, 10, 10}, Supervisors}}.
