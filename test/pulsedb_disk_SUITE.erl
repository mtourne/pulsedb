-module(pulsedb_disk_SUITE).
-compile(export_all).

-include("../src/pulsedb.hrl").

all() ->
  [{group, disk}].

groups() ->
  [{disk, [parallel], [
    append_and_read
  ]}].



append_and_read(_) ->
  {ok, DB1} = pulsedb_disk:open("test/v1/append_and_read"),
  Ticks = [
    #tick{name = <<"source1">>, utc = 121, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 122, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 123, value = [{input,3},{output,6}]}
  ],
  {ok, DB2} = pulsedb_disk:append(Ticks, DB1),
  pulsedb_disk:close(DB2),

  {ok, DB3} = pulsedb_disk:open("test/v1/append_and_read"),
  {ok, Ticks, DB4} = pulsedb_disk:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-01"}], DB3),
  pulsedb_disk:close(DB4),
  ok.


