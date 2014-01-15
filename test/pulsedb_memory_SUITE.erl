-module(pulsedb_memory_SUITE).
-compile(export_all).

all() ->
  [
    {group, memory}
  ].


groups() ->
  [{memory, [], [
    read_and_write_from_memory
    % passive_flow_init,
    % passive_flow_collect,
    % passive_flow_flush,
    % passive_flow_flush_hour,

    % active_flow_collect,
    % update_pulse_configuration,

    % pulse_subscribe,
    % pulse_unsubscribe
  ]}].

init_per_suite(Config) ->
  application:start(pulsedb),
  meck:new([pulsedb], [{passthrough,true}, {no_link,true}]),
  Config.


end_per_suite(Config) ->
  application:stop(pulsedb),
  meck:unload([pulsedb]),
  Config.



read_and_write_from_memory(_) ->
  T = flu:now() - 120,

  pulsedb_memory:append([
    {<<"input">>, T+0, 10, [{<<"name">>,<<"src1">>}]},
    {<<"input">>, T+0, 14, [{<<"name">>,<<"src2">>}]},
    {<<"input">>, T+1, 04, [{<<"name">>,<<"src1">>}]},
    {<<"input">>, T+1, 08, [{<<"name">>,<<"src2">>}]}
  ], seconds),
  {ok, [
    {_,24},
    {_,12}
  ], _} = pulsedb:read("sum:input{from="++integer_to_list(T)++",to="++integer_to_list(T+10)++"}", seconds),


  T1 = case ((T+10) div 60)*60 of
    T_ when T_ < T -> T_+60;
    T_ -> T_
  end,
  pulsedb_memory:merge_seconds_data([{<<"input">>, [{<<"name">>,<<"src1">>}]}], T1),

  {ok, [
    {_,7}
  ], _} = pulsedb:read("sum:input{from="++integer_to_list(T1-120)++",to="++integer_to_list(T1+60)++",name=src1}", minutes),


  pulsedb_memory:append([
    {<<"input">>, T-3600, 10, [{<<"name">>,<<"src1">>}]}
  ], seconds),

  {ok, [
    {_,10}
  ], _} = pulsedb:read("sum:input{from="++integer_to_list(T-3620)++",to="++integer_to_list(T-3500)++"}", seconds),

  whereis(pulsedb_memory) ! clean,
  sys:get_state(pulsedb_memory),

  {ok, [
  ], _} = pulsedb:read("sum:input{from="++integer_to_list(T-3620)++",to="++integer_to_list(T-3500)++"}", seconds),

  ok.












passive_flow_collect(_) ->
  meck:expect(pulse, current_second, fun() ->
    {42, 900}
  end),
  pulse:new_passive("disk2", [read, write]),
  [{read,[]},{write,[]}] = pulse:history("disk2", seconds),

  pulse:append("disk2", [{read, 10}, {write, 30}]),
  [{read, [{42,10}]}, {write, [{42,30}]}] = pulse:history("disk2", seconds),

  pulse:append("disk2", [{read, 10}, {write, 30}]),
  [{read, [{42,20}]}, {write, [{42,60}]}] = pulse:history("disk2", seconds),

  meck:expect(pulse, current_second, fun() ->
    {43, 900}
  end),

  pulse:append("disk2", [{read, 10}, {write, 30}]),
  [{read, [{42,20},{43,10}]}, {write, [{42,60},{43,30}]}] = pulse:history("disk2", seconds),
  ok.


passive_flow_flush(_) ->
  meck:expect(pulse, current_second, fun() -> {22, 900} end),
  meck:expect(pulse, current_minute, fun() -> {0, 9000} end),
  {ok, P} = pulse:new_passive("disk3", [read, write]),

  meck:expect(pulse, current_second, fun() -> {22, 900} end),
  pulse:append("disk3", [{read, 10}, {write, 30}]),

  meck:expect(pulse, current_second, fun() -> {23, 900} end),
  pulse:append("disk3", [{read, 15}, {write, 20}]),


  meck:expect(pulse, current_minute, fun() -> {60, 9000} end),
  meck:expect(pulse, current_second, fun() -> {103, 900} end),
  pulse:append("disk3", [{read, 10}, {write, 20}]),

  P ! flush,
  sys:get_status(P),

  [{read, [{103,10}]}, {write, [{103,20}]}] = pulse:history("disk3", seconds),
  [{read, [{0,12.5}]}, {write, [{0,25.0}]}] = pulse:history("disk3", minutes),



  meck:expect(pulse, current_minute, fun() -> {120, 9000} end),
  meck:expect(pulse, current_second, fun() -> {173, 900} end),
  pulse:append("disk3", [{read, 10}, {write, 20}]),

  P ! flush,
  sys:get_status(P),

  [{read, [{173,10}]}, {write, [{173,20}]}] = pulse:history("disk3", seconds),
  [{read, [{0,12.5},{60,10.0}]}, {write, [{0,25.0},{60,20.0}]}] = pulse:history("disk3", minutes),
  ok.








passive_flow_flush_hour(_) ->
  T = 6*3600,

  meck:expect(pulse, current_second, fun() -> {22, 900} end),
  meck:expect(pulse, current_minute, fun() -> {0, 9000} end),
  {ok, P} = pulse:new_passive("disk3", [read, write]),

  meck:expect(pulse, current_second, fun() -> {22, 900} end),
  pulse:append("disk3", [{read, 10}, {write, 30}]),

  meck:expect(pulse, current_minute, fun() -> {T+120, 9000} end),
  meck:expect(pulse, current_second, fun() -> {T+125, 900} end),
  pulse:append("disk3", [{read, 10}, {write, 20}]),


  meck:expect(pulse, current_minute, fun() -> {T+3660, 9000} end),
  meck:expect(pulse, current_second, fun() -> {T+3703, 900} end),
  pulse:append("disk3", [{read, 10}, {write, 20}]),

  P ! flush,
  sys:get_status(P),

  R1 = [{read, [{T+3703,10}]}, {write, [{T+3703,20}]}],
  R1 = pulse:history("disk3", seconds),
  _R2 = [{read, [{T+3600,0}]}, {write, [{T+3600,0}]}],

  ok.











active_flow_collect(_) ->
  meck:new(active_flow_source),
  meck:expect(active_flow_source, pulse_init, fun(V) -> {ok, V} end),
  meck:expect(active_flow_source, pulse_collect, fun(Columns, V) ->
    {reply, [{C,V} || C <- Columns], V}
  end),

  {ok, P} = pulse:new_active("net1", [input,output], {active_flow_source, 5}),

  meck:expect(pulse, current_second, fun() -> {20, 900} end),

  P ! collect,
  sys:get_status(P),

  [{input, [{20,5}]}, {output, [{20,5}]}] = pulse:history("net1", seconds),

  P ! collect,
  sys:get_status(P),

  [{input, [{20,10}]}, {output, [{20,10}]}] = pulse:history("net1", seconds),

  meck:expect(pulse, current_second, fun() -> {21, 900} end),
  P ! collect,
  sys:get_status(P),

  [{input, [{20,10},{21,5}]}, {output, [{20,10},{21,5}]}] = pulse:history("net1", seconds),

  erlang:exit(P, normal),
  meck:unload(active_flow_source),
  ok.




update_pulse_configuration(_) ->
  meck:new(source1),
  meck:expect(source1, pulse_collect, fun(Columns, V) ->
    {reply, [{C,V} || C <- Columns], V}
  end),

  {ok, P} = pulse:new_active("net4", [input,output], {source1, 5}),

  meck:expect(pulse, current_second, fun() -> {20, 900} end),

  P ! collect,
  sys:get_status(P),

  [{input, [{20,5}]}, {output, [{20,5}]}] = pulse:history("net4", seconds),

  pulse:update_config("net4", [input, delay, output]),
  [{input, [{20,5}]}, {delay, [{20,0}]}, {output, [{20,5}]}] = pulse:history("net4", seconds),

  P ! collect,
  sys:get_status(P),

  [{input, [{20,10}]}, {delay, [{20,5}]}, {output, [{20,10}]}] = pulse:history("net4", seconds),

  meck:unload(source1),
  ok.



pulse_subscribe(_) ->
  {ok, P} = pulse:new_passive(<<"disk3.1">>, [read, write]),
  pulse:subscribe(<<"disk3.1:read,write">>),
  pulse:append(<<"disk3.1">>, [{read, 10}, {write, 30}]),
  receive
    {pulse, <<"disk3.1">>, second, _, Values1} ->
      10 = proplists:get_value(read,Values1),
      30 = proplists:get_value(write,Values1)
  after
    10 -> error(no_pulse_messages)
  end,

  P ! flush,
  receive
    {pulse, <<"disk3.1">>, minute, _, _Values2} -> ok
  after
    10 -> error(no_pulse_messages)
  end,
  ok.  


pulse_unsubscribe(_) ->
  {ok, _P} = pulse:new_passive(<<"disk3.1">>, [read, write]),
  pulse:subscribe(<<"disk3.1">>),
  pulse:unsubscribe(<<"disk3.1">>),
  pulse:append(<<"disk3.1">>, [{read, 10}, {write, 30}]),
  receive
    {pulse, <<"disk3.1">>, second, _, _Values} -> error(still_subscribed)
  after
    10 -> ok
  end.





passive_gauge_init(_) ->
  pulse:new_passive("disk5", [total, used]),
  [{total,undefined},{used,undefined}] = pulse:gauge("disk5").



passive_gauge_collect(_) ->
  meck:expect(pulse, current_second, fun() -> {42, 900} end),

  pulse:new_passive("disk4", [total, used]),
  [{total,undefined},{used,undefined}] = pulse:gauge("disk4"),

  pulse:set("disk4", [{total, 30}, {used, 10}]),
  [{total, 30}, {used, 10}] = pulse:gauge("disk4"),

  pulse:set("disk4", [{total, 30}, {used, 20}]),
  [{total, 30}, {used, 20}] = pulse:gauge("disk4"),

  meck:expect(pulse, current_second, fun() -> {43, 900} end),
  pulse:set("disk4", [{total, 15}, {used, 5}]),
  [{total, 15}, {used, 5}] = pulse:gauge("disk4"),

  [{total, [{42,30}, {43,15}]}, {used, [{42,20},{43,5}]}] = pulse:history("disk4", seconds),

  ok.





