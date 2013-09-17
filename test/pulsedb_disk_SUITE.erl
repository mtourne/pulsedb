-module(pulsedb_disk_SUITE).
-compile(export_all).

-include("../src/pulsedb.hrl").
-include("../include/pulsedb.hrl").

all() ->
  [{group, disk}].

groups() ->
  [{disk, [parallel], [
    append_and_read
  ]}].



append_and_read(_) ->
  {ok, DB1} = pulsedb_disk:open("test/v1/append_and_read"),
  Ticks1 = [
    #tick{name = <<"source1">>, utc = 120, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 130, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 140, value = [{input,3},{output,6}]}
  ],
  {ok, DB2} = pulsedb_disk:append(Ticks1, DB1),

  Ticks2 = [
    #tick{name = <<"source1">>, utc = 4000121, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 4000122, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 4000123, value = [{input,3},{output,6}]}
  ],
  {ok, DB3} = pulsedb_disk:append(Ticks2, DB2),

  pulsedb_disk:close(DB3),

  {ok, ReadDB1} = pulsedb_disk:open("test/v1/append_and_read"),
  {ok, Ticks1, ReadDB2} = pulsedb_disk:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], ReadDB1#db{date = <<"1970/01/01">>}),

  pulsedb_disk:close(ReadDB2),
  ok.


