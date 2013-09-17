-module(pulsedb_SUITE).
-compile(export_all).

-include("../src/pulsedb.hrl").


all() ->
  [{group, append_and_read}].


groups() ->
  [{append_and_read, [parallel], [
    append_and_read
  ]}].

append_and_read(_) ->
  {ok, DB0} = pulsedb:open("test/v1/pulse_rw"),

  Ticks1 = [
    #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 23, value = [{input,3},{output,6}]}
  ],
  {ok, DB1} = pulsedb:append(Ticks1, DB0),

  Ticks2 = [
    #tick{name = <<"source1">>, utc = 172821, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 172822, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 172823, value = [{input,3},{output,6}]}
  ],
  {ok, DB2} = pulsedb:append(Ticks2, DB1),
  pulsedb:close(DB2),


  {ok, WDB0} = pulsedb:open("test/v1/pulse_rw"),
  {ok, Ticks1, DB3} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], WDB0),
  {ok, Ticks2, DB4} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-03"},{to,"1970-01-04"}], DB3),

  Ticks3 = Ticks1++Ticks2,
  {ok, Ticks3, DB5} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-04"}], DB4),

  pulsedb:close(DB5).


