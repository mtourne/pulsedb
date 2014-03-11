-module(pulsedb_aggregator).
-compile(export_all).

-export([aggregate/5]).

aggregate(Resolution, UtcFrom, UtcTo, Source0, Target0) ->
  TargetResolution = next_layer(Resolution),
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


next_layer(seconds) -> minutes;
next_layer(minutes) -> hours;
next_layer(_) -> undefined.

interval(minutes) -> 60;
interval(hours) -> 3600.

make_ticks(Name0, Tags, {_, Downsampler}=Ds, Ticks0) ->
  Ticks1 = pulsedb_data:downsample(Ds, Ticks0),
  Name = <<Downsampler/binary, "-", Name0/binary>>,
  [{Name, UTC, Value, Tags} || {UTC, Value} <- Ticks1].
