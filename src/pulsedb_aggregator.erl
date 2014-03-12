-module(pulsedb_aggregator).
-compile(export_all).

-export([aggregate/5]).




aggregate(TargetResolution, UtcFrom, UtcTo, Source0, Target0) ->
  Interval = interval(TargetResolution),
  Aggregator = {aggregator, <<"sum">>},
  
  {ok, Data, _} = pulsedb_disk:read_all(UtcFrom, UtcTo, [Aggregator], Source0),
  Target1 = 
  lists:foldl(fun ({{Name, Tags}, Ticks}, T0) ->
                TicksAvg = make_ticks(Name, Tags, {Interval, <<"avg">>}, Ticks),
                TicksMax = make_ticks(Name, Tags, {Interval, <<"max">>}, Ticks),
                {ok, T1} = pulsedb:append(TicksAvg, T0),
                {ok, T2} = pulsedb:append(TicksMax, T1),
                T2
              end, Target0, Data),
  {ok, Target1}.


interval(minutes) -> 60;
interval(hours) -> 3600.

make_ticks(Name0, Tags, {_, Downsampler}=Ds, Ticks0) ->
  Ticks1 = pulsedb_data:downsample(Ds, Ticks0),
  Prefix = prefix(Downsampler),
  Name = <<Prefix/binary, Name0/binary>>,
  [{Name, UTC, Value, Tags} || {UTC, Value} <- Ticks1].


prefix(<<"avg">>) -> <<>>;
prefix(DS) when is_binary(DS) -> <<DS/binary, "-">>;
prefix(DS) -> iolist_to_binary(lists:concat([DS, "-"])).
