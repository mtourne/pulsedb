-module(pulsedb_tests).
-compile(export_all).

write_test() ->
  os:cmd("rm -rf user15/eth0"),
  {ok, P1} = pulsedb:open_append("user15/eth0"),
  {ok, P2} = pulsedb:append({row, 1368872568737, [45,23]}, P1),
  {ok, P3} = pulsedb:append({row, 1368872568747, [40,27]}, P2),

  {ok, _} = file:read_file_info("user15/eth0/2013/05/18.pulse"),

  {ok, P4} = pulsedb:append({row, 1368958968747, [40,27]}, P3),
  pulsedb:close(P4),
  {ok, _} = file:read_file_info("user15/eth0/2013/05/19.pulse"),

  [{row, 1368872568737, [45,23]},
  {row, 1368872568747, [40,27]}] =
    pulsedb:events("user15/eth0", "2013-05-18"),

  [{row, 1368958968747, [40,27]}] =
    pulsedb:events("user15/eth0", "2013-05-19"),
  ok.




write2_test() ->
  os:cmd("rm -rf write-test5"),
  {ok, P1} = pulsedb:open_append("write-test5"),
  {ok, P2} = pulsedb:append({row, 1368872568737, [{<<"input">>,45},{output,23}]}, P1),
  {ok, P3} = pulsedb:append({row, 1368872568747, [{input,40},{<<"output">>,27}]}, P2),
  pulsedb:close(P3),

  [{<<"input">>, [
    {1368872568737, 45},
    {1368872568747, 40}
  ]},
  {<<"output">>, [
    {1368872568737,23},
    {1368872568747,27}
  ]}] = pulsedb:event_columns("write-test5", "2013-05-18"),
  ok.



nofile_test() ->
  [] = pulsedb:events("user5/eth1", "2013-05-19"),
  [] = pulsedb:event_columns("user5/eth1", "2013-05-19"),
  ok.
