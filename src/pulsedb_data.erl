-module(pulsedb_data).
-export([shift_value/1, unshift_value/1, unpack_ticks/2, unpack_ticks/3]).
-export([interpolate/4, aggregate/2, downsample/2]).


aggregate(Aggegator, Ticks) ->
  Agg = case Aggegator of
    undefined -> fun sum/1;
    <<"sum">> -> fun sum/1;
    <<"avg">> -> fun avg/1;
    <<"max">> -> fun max/1;
    Else -> error({unknown_aggregator,Else})
  end,
  aggregate(Agg, Ticks, undefined, []).


aggregate(_Agg, [], undefined, []) -> [];
aggregate(Agg, [], UTC, Acc) -> [{UTC,Agg(Acc)}];
aggregate(Agg, [{UTC,V1}|Ticks], undefined, []) -> aggregate(Agg, Ticks, UTC, [V1]);
aggregate(Agg, [{UTC,V1}|Ticks], UTC, Acc) -> aggregate(Agg, Ticks, UTC, [V1|Acc]);
aggregate(Agg, [{UTC2,V1}|Ticks], UTC1, Acc) ->
  [{UTC1,Agg(Acc)}|aggregate(Agg, Ticks, UTC2, [V1])].


sum([]) -> 0;
sum(Acc) -> lists:sum(Acc).

avg([]) -> 0;
avg(Acc) -> lists:sum(Acc) div length(Acc).

max([]) -> 0;
max(Acc) -> lists:max(Acc).




downsample(undefined, Ticks) ->
  Ticks;

downsample({Step, Downsampler}, Ticks) ->
  D = case Downsampler of
    undefined -> fun sum/1;
    <<"sum">> -> fun sum/1;
    <<"avg">> -> fun avg/1;
    <<"max">> -> fun max/1;
    Else -> error({unknown_aggregator,Else})
  end,
  downsample(D, Ticks, Step, undefined, []).


downsample(_D, [], _Step, undefined, []) -> [];
downsample(D, [], _Step, UTC, Acc) -> [{UTC,D(Acc)}];
downsample(D, [{UTC,V}|Ticks], Step, undefined, []) -> downsample(D, Ticks, Step, (UTC div Step)*Step, [V]);
downsample(D, [{UTC,V}|Ticks], Step, N, Acc) when (UTC div Step)*Step == N -> downsample(D, Ticks, Step, N, [V|Acc]);
downsample(D, [{UTC,V}|Ticks], Step, N, Acc) -> [{N,D(Acc)}|downsample(D, Ticks, Step, (UTC div Step)*Step, [V])].



shift_value(Value) when Value >= 0              andalso Value < 16#4000 -> <<3:2, Value:14>>;
shift_value(Value) when Value >= 16#1000        andalso Value < 16#1000000 -> <<2:2, (Value bsr 10):14>>;
shift_value(Value) when Value >= 16#100000      andalso Value < 16#400000000 -> <<1:2, (Value bsr 20):14>>;
shift_value(Value) when Value >= 16#10000000    andalso Value < 16#80000000000 -> <<0:2, 1:1, (Value bsr 30):13>>;
shift_value(Value) when Value >= 16#10000000000 andalso Value < 16#20000000000000 -> <<0:2, 0:1, (Value bsr 40):13>>.


unshift_value(Bin) when is_binary(Bin) ->
  [{_,V}] = unpack_ticks(Bin, 0),
  V.

unpack_ticks(Data, UTC) ->
  unpack_ticks(Data, UTC, 1).

unpack_ticks(<<>>, _, _) -> [];
unpack_ticks(<<0:16, Rest/binary>>, UTC, Step) -> unpack_ticks(Rest, UTC+Step, Step);
unpack_ticks(<<3:2, Value:14, Rest/binary>>, UTC, Step) -> [{UTC,Value}|unpack_ticks(Rest, UTC+Step, Step)];
unpack_ticks(<<2:2, Value:14, Rest/binary>>, UTC, Step) -> [{UTC,Value bsl 10}|unpack_ticks(Rest, UTC+Step, Step)];
unpack_ticks(<<1:2, Value:14, Rest/binary>>, UTC, Step) -> [{UTC,Value bsl 20}|unpack_ticks(Rest, UTC+Step, Step)];
unpack_ticks(<<0:2, 1:1, Value:13, Rest/binary>>, UTC, Step) -> [{UTC,Value bsl 30}|unpack_ticks(Rest, UTC+Step, Step)];
unpack_ticks(<<0:2, 0:1, Value:13, Rest/binary>>, UTC, Step) -> [{UTC,Value bsl 40}|unpack_ticks(Rest, UTC+Step, Step)].


interpolate(_UTC, 0, _Step, []) -> [];
interpolate(UTC, Count, Step, [{UTC, Value}|Rest]) -> [{UTC, Value}|interpolate(UTC+Step, Count-1, Step, Rest)];
interpolate(UTC, Count, Step, Rest)                -> [{UTC, 0}    |interpolate(UTC+Step, Count-1, Step, Rest)].
