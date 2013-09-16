-module(pulsedb_format_SUITE).
-compile(export_all).

-include("../src/pulsedb.hrl").

all() ->
  [{group, format}].

groups() ->
  [{format, [parallel], [
    config,
    data
  ]}].


config(_) ->
  Src1 = #source{source_id = 0, name = <<"src1">>, columns = [a,b,c]},

  Bin1 = pulsedb_format:encode_config(Src1),
  Bin1 = pulsedb_format:encode_config([Src1]),
  [Src1] = pulsedb_format:decode_config(Bin1),
  ok.



data(_) ->
  Ticks = [
    #tick{name = <<"source1">>, utc = 121, value = [{input,5},{output,0}]},
    #tick{name = <<"source1">>, utc = 122, value = [{input,10},{output,2}]},
    #tick{name = <<"source1">>, utc = 123, value = [{input,3},{output,6}]}
  ],
  Source = #source{source_id = 1, name = <<"source1">>, columns = [input,output]},

  Bin = iolist_to_binary(pulsedb_format:encode_data(Source, Ticks)),

  Ticks = pulsedb_format:decode_data([Source], Bin),
  ok.
