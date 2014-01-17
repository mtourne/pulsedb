-module(pulsedb_realtime_SUITE).
-compile(export_all).
-define(TICK_COUNT, 5).


all() ->
  [{group, subscribe}].


groups() ->
  [{subscribe, [parallel], [
    subscribe,
    unsubscribe
  ]}].


init_per_suite(Config) ->
  application:start(pulsedb),
  Config.


end_per_suite(Config) ->
  application:stop(pulsedb),
  Config.

subscribe(_) ->
  N = ?TICK_COUNT,
  {UTC,_} = pulsedb:current_second(),
  spawn(fun () -> send_tick(N, {<<"input">>, UTC, 10, [{<<"name">>,<<"src1">>}]}) end),
  pulsedb_realtime:subscribe(<<"input">>, i),
  ok = collect_ticks(N,UTC,1200).


unsubscribe(_) ->
  N = ?TICK_COUNT,
  {UTC,_} = pulsedb:current_second(),
  spawn(fun () -> send_tick(N, {<<"input">>, UTC, 10, [{<<"name">>,<<"src1">>}]}) end),
  pulsedb_realtime:subscribe(<<"input">>, i),
  ok = collect_ticks(N,UTC,1200),
  pulsedb_realtime:unsubscribe(i),
  send_tick(N, {<<"input">>, UTC+6, 10, [{<<"name">>,<<"src1">>}]}),
  receive
    _ -> error(received_pulse)
  after 2000 -> ok 
  end.
  
  
send_tick(0, _) -> ok;
send_tick(N, {Name, UTC, Value, Tags}=Pulse) ->
  pulsedb_memory:append(Pulse, seconds),
  Pulse1 = {Name, UTC+1, Value, Tags},
  timer:sleep(1000),
  send_tick(N-1, Pulse1).
  

collect_ticks(0,_,_) -> ok;
collect_ticks(N,UTC,Timeout) ->
  receive
    {pulse, i, UTC,_} -> collect_ticks(N-1,UTC+1,Timeout)
  after
    Timeout -> error(collect_timed_out)
  end.
