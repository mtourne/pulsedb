-module(pulsedb_format_tests).
-include("../include/pulsedb.hrl").
-include("pulsedb.hrl").

% EUnit tests
-include_lib("eunit/include/eunit.hrl").


full_row_test() ->
  Timestamp = 15343556,
  Values = [250, 17,5435,5430,23],
  Bin = <<0:1, 0:1, 5:8, 15343556:54, 250:32, 17:32, 5435:32, 5430:32, 23:32>>,
  BinSize = byte_size(Bin),
  Tail = <<7, 239, 183, 19>>,

  Bin = pulsedb_format:encode_full_row(Timestamp, Values),
  {Timestamp, Values, BinSize} = pulsedb_format:decode_full_row(Bin),

  {ok, {row,Timestamp,Values}, BinSize} = pulsedb_format:decode_packet(Bin, undefined),
  ok.


full_row_negative_test() ->
  Timestamp = 15343556,
  Values = [250, -17,5435,-5430,23],
  Bin = <<0:1, 0:1, 5:8, 15343556:54, 250:32, -17:32, 5435:32, -5430:32, 23:32>>,
  BinSize = byte_size(Bin),
  Tail = <<7, 239, 183, 19>>,

  Bin = pulsedb_format:encode_full_row(Timestamp, Values),
  {Timestamp, Values, BinSize} = pulsedb_format:decode_full_row(Bin),

  {ok, {row,Timestamp,Values}, BinSize} = pulsedb_format:decode_packet(Bin, undefined),
  ok.



delta_row_test() ->
  TSDelta = 15,
  ValuesDelta = [34, 0,8,15,-8],

  Bin = pulsedb_format:encode_delta_row(TSDelta, ValuesDelta),
  BinSize = size(Bin),
  {TSDelta, ValuesDelta, BinSize} = pulsedb_format:decode_delta_row(Bin, length(ValuesDelta)),
  ok.

delta_row1_test() ->
  TSDelta = 15,
  ValuesDelta = [34, 0,8,0,15,-8],

  Bin = pulsedb_format:encode_delta_row(TSDelta, ValuesDelta),
  BinSize = size(Bin),
  {TSDelta, ValuesDelta, BinSize} = pulsedb_format:decode_delta_row(Bin, length(ValuesDelta)),
  ok.


