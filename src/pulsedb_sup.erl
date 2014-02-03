-module(pulsedb_sup).
-export([start_link/0, stop/0, init/1]).
-export([start_collector/4]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_collector(Name, Module, Args, Options) ->
  Spec = {Name, {pulsedb_collector, start_link, [Name, Module, Args, Options]}, transient, 200, worker, []},
  case supervisor:start_child(pulsedb_collectors, Spec) of
    {ok, Pid} -> {ok, Pid};
    {error, already_present} -> supervisor:restart_child(pulsedb_collectors, Name);
    {error, {already_started, Pid}} -> {ok, Pid}
  end.



stop() ->
  erlang:exit(erlang:whereis(?MODULE), shutdown).

init([pulsedb_collectors]) ->
  {ok, {{one_for_one, 5, 10}, []}};


init([]) ->
  Supervisors = [
    {pulsedb_memory,     {pulsedb_memory, start_link, []}, permanent, 100, worker, []},
    {pulsedb_netpushers, {gen_tracker, start_link, [pulsedb_netpushers]}, permanent, infinity, supervisor, []},
    {pulsedb_collectors, {supervisor, start_link, [{local,pulsedb_collectors}, ?MODULE, [pulsedb_collectors]]}, permanent, infinity, supervisor, []},
    {pulsedb_realtime, {pulsedb_realtime, start_link, []}, permanent, 100, worker, []},
    {pulsedb_embed_cache, {pulsedb_embed_cache, start_link, []}, permanent, 100, worker, []},
    {pulsedb_pushers,    {gen_tracker, start_link, [pulsedb_pushers]}, permanent, 2000, supervisor, []}
  ],
  {ok, {{one_for_one, 10, 10}, Supervisors}}.
