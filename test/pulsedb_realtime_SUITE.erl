-module(pulsedb_realtime_SUITE).
-compile(export_all).
-define(TICK_COUNT, 5).


all() ->
  [{group, subscribe}].


groups() ->
  [{subscribe, [parallel], [
    subscribe
    % subscribe_twice,
    % unsubscribe
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
  pulsedb_realtime:subscribe(<<"input">>, i),
  Self = self(),
  spawn_link(fun () -> send_tick(100, {<<"input">>, UTC-100, 1, [{<<"name">>,<<"src1">>}]}), Self ! go end),
  receive go -> ok after 1000 -> error(timeout) end,
  ok = collect_ticks(N,500).


% subscribe_twice(_) ->
%   N = ?TICK_COUNT,
%   {UTC,_} = pulsedb:current_second(),
%   pulsedb_realtime:subscribe(<<"input">>, i),
%   pulsedb_realtime:subscribe(<<"input">>, i),
%   Self = self(),
%   spawn_link(fun () -> send_tick(100, {<<"input">>, UTC-100, 1, [{<<"name">>,<<"src1">>}]}), Self ! go end),
%   receive go -> ok after 1000 -> error(timeout) end,
%   ok = collect_ticks(N,500),
%   receive
%     _ -> error(received_pulse)
%   after 2000 -> ok 
%   end.

% unsubscribe(_) ->
%   N = ?TICK_COUNT,
%   {UTC,_} = pulsedb:current_second(),
%   pulsedb_realtime:subscribe(<<"input">>, i),
%   Self = self(),
%   spawn(fun () -> send_tick(100, {<<"input">>, UTC-100, 1, [{<<"name">>,<<"src1">>}]}), Self ! go end),
%   receive go -> ok after 1000 -> error(timeout) end,

%   ok = collect_ticks(N,500),
%   pulsedb_realtime:unsubscribe(i),
%   send_tick(N, {<<"input">>, UTC+6, 10, [{<<"name">>,<<"src1">>}]}),
%   receive
%     _ -> error(received_pulse)
%   after 2000 -> ok 
%   end.
  
  
send_tick(0, _) -> ok;
send_tick(N, {Name, UTC, Value, Tags}=Pulse) ->
  pulsedb_memory:append(Pulse, seconds),
  Pulse1 = {Name, UTC+1, Value, Tags},
  timer:sleep(2),
  send_tick(N-1, Pulse1).
  

collect_ticks(0,_) -> ok;
collect_ticks(N,Timeout) ->
  receive
    {pulse, i, _,_} -> collect_ticks(N-1,Timeout)
  after
    Timeout -> ct:pal("messages: ~p", [process_info(self(),messages)]), error(collect_timed_out)
  end.
