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




