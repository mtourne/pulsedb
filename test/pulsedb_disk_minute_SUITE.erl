-module(pulsedb_disk_minute_SUITE).
-compile(export_all).



all() ->
  [{group, append_and_read}].


groups() ->
  [{append_and_read, [parallel], [
    append_and_read
  ]}].



key() -> <<"0123456789abcdef">>.

start_app(App) ->
  case application:start(App) of
    ok -> {ok, App};
    {error,{already_started,App}} -> {ok, App};
    Other -> Other
  end.

init_per_suite(Config) ->
  application:start(pulsedb),
  Config.


end_per_suite(Config) ->
  application:stop(pulsedb),
  Config.


append_and_read(_) ->
  {ok, DB1} = pulsedb:open("test/pulse_rw_minute", [{resolution, minutes}]),

  Ticks1 = [
    {<<"input">>, 120, 5, [{name, <<"source1">>}]},
    {<<"output">>, 120, 0, [{name, <<"source1">>}]},

    {<<"input">>, 120, 2, [{name, <<"source2">>}]},
    {<<"output">>, 120, 2, [{name, <<"source2">>}]},


    {<<"input">>, 180, 10, [{name, <<"source1">>}]},
    {<<"output">>, 180, 2, [{name, <<"source1">>}]},

    {<<"input">>, 240, 3, [{name, <<"source1">>}]},
    {<<"output">>, 240, 6, [{name, <<"source1">>}]}
  ],
  {ok, DB2} = pulsedb:append(Ticks1, DB1),

  Ticks2 = [
    {<<"input">>, 4000140, 5, [{name, <<"source1">>}]},
    {<<"output">>, 4000140, 0, [{name, <<"source1">>}]},

    {<<"input">>, 4000200, 10, [{name, <<"source1">>}]},
    {<<"output">>, 4000200, 2, [{name, <<"source1">>}]},

    {<<"input">>, 4000200, 4, [{name, <<"source2">>}]},
    {<<"output">>, 4000200, 4, [{name, <<"source2">>}]},

    {<<"input">>, 4000260, 3, [{name, <<"source1">>}]},
    {<<"output">>, 4000260, 6, [{name, <<"source1">>}]}
  ],
  {ok, DB3} = pulsedb:append(Ticks2, DB2),

  pulsedb:close(DB3),

  {ok, ReadDB1} = pulsedb:open("test/pulse_rw_minute", [{resolution, minutes}]),

  {ok, [
    {120,5},
    {180,10},
    {240,3},
    {4000140,5},
    {4000200,10},
    {4000260,3}
  ], ReadDB2} = pulsedb:read(<<"input">>, [{name,<<"source1">>}, {from, "1970-01-01"},{to,"1971-02-02"}], ReadDB1),


  {ok, [
    {120,5},
    {180,10},
    {240,3}
  ], ReadDB3} = pulsedb:read(<<"input">>, [{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], ReadDB2),

  {ok, [
    {120,7},
    {180,10},
    {240,3},
    {4000140,5},
    {4000200,14},
    {4000260,3}
  ], ReadDB4} = pulsedb:read(<<"input">>, [{from, "1970-01-01"},{to,"1971-01-02"}], ReadDB3),
  ok.

