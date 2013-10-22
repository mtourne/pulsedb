-module(pulsedb_SUITE).
-compile(export_all).

-include("../include/pulsedb.hrl").


all() ->
  [{group, append_and_read}].


groups() ->
  [{append_and_read, [parallel], [
    append_and_read,
    forbid_to_read_after_append,
    forbid_to_append_after_read,
    merge,
    info
  ]}].

append_and_read(_) ->
  {ok, DB0} = pulsedb:open("test/v2/pulse_rw"),

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


  {ok, WDB0} = pulsedb:open("test/v2/pulse_rw"),
  {ok, Ticks1, DB3} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], WDB0),
  {ok, Ticks2, DB4} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-03"},{to,"1970-01-04"}], DB3),

  Ticks3 = Ticks1++Ticks2,
  {ok, Ticks3, DB5} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-04"}], DB4),
  pulsedb:close(DB5),

  os:cmd("rm -rf test/v2/pulse_rw/1970/01/01"),

  {ok, R1} = pulsedb:open("test/v2/pulse_rw"),
  {ok, Ticks2, R2} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-04"}], R1),
  pulsedb:close(R2).


forbid_to_read_after_append(_) ->
  {ok, DB0} = pulsedb:open("test/v2/forbid_to_read"),

  Ticks1 = [
    #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 23, value = [{input,3},{output,6}]}
  ],
  {ok, DB1} = pulsedb:append(Ticks1, DB0),

  {error, _} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], DB1),
  ok.


forbid_to_append_after_read(_) ->
  {ok, DB0} = pulsedb:open("test/v2/forbid_to_append"),

  Ticks1 = [
    #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 23, value = [{input,3},{output,6}]}
  ],
  {ok, DB1} = pulsedb:append(Ticks1, DB0),
  pulsedb:close(DB1),

  {ok, DB2} = pulsedb:open("test/v2/forbid_to_append"),
  {ok, _, DB3} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], DB2),
  {error, _} = pulsedb:append(Ticks1, DB3),

  ok.


info(_) ->
  {ok, DB0} = pulsedb:open("test/v2/info"),

  Ticks1 = [
    #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 23, value = [{input,3},{output,6}]}
  ],
  {ok, DB1} = pulsedb:append(Ticks1, DB0),

  Ticks2 = [
    #tick{name = <<"source2">>, utc = 21, value = [{x,5},{y,0}]},
    #tick{name = <<"source2">>, utc = 22, value = [{x,10},{y,2}]},
    #tick{name = <<"source2">>, utc = 23, value = [{x,3},{y,6}]}
  ],
  {ok, DB2} = pulsedb:append(Ticks1, DB1),

  Info1 = pulsedb:info(DB2),
  {_,Sources1} = lists:keyfind(sources,1,Info1),
  {_,S1} = lists:keyfind(<<"source1">>,1,Sources1),
  {_,Columns1} = lists:keyfind(columns,1,S1),
  [input,output] = Columns1,

  pulsedb:close(DB2),

  Info2 = pulsedb:info("test/v2/info"),
  {_,Sources2} = lists:keyfind(sources,1,Info2),
  {_,S2} = lists:keyfind(<<"source1">>,1,Sources2),
  {_,Columns2} = lists:keyfind(columns,1,S2),
  [input,output] = Columns2,
  ok.







merge(_) ->
  {ok, DB0} = pulsedb:open("test/v2/merge"),

  Ticks1 = [
    #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 23, value = [{input,3},{output,6}]}
  ],
  {ok, DB1} = pulsedb:append(Ticks1, DB0),

  Ticks2 = [
    #tick{name = <<"source1">>, utc = 11, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 12, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 13, value = [{input,3},{output,6}]}
  ],
  {ok, DB2} = pulsedb:append(Ticks2, DB1),
  pulsedb:close(DB2),


  Ticks3 = Ticks2++Ticks1,
  {ok, DB3} = pulsedb:open("test/v2/merge"),
  {ok, Ticks3, DB4} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], DB3),
  pulsedb:close(DB4),

  ok.












