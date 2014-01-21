-module(pulsedb_time).

-export([daystart/1, date_time/1]).


-export([parse/1, date_path/1]).



to_i(L) when is_list(L) -> list_to_integer(L);
to_i(Bin) when is_binary(Bin) -> binary_to_integer(Bin).


parse(UTC) when is_integer(UTC) ->
  UTC;

parse(String) when is_list(String) ->
  parse(iolist_to_binary(String));

parse({Y,M,D}) ->
  utc({{Y,M,D},{0,0,0}});

parse(<<Y:4/binary, "-", Mon:2/binary, "-", D:2/binary>>) ->
  utc({{to_i(Y), to_i(Mon), to_i(D)}, {0,0,0}});

parse(<<Y:4/binary, "/", Mon:2/binary, "/", D:2/binary>>) ->
  utc({{to_i(Y), to_i(Mon), to_i(D)}, {0,0,0}});

parse(<<Y:4/binary, "-", Mon:2/binary, "-", D:2/binary, " ", H:2/binary, ":", Min:2/binary, ":", S:2/binary>>) ->
  utc({{to_i(Y), to_i(Mon), to_i(D)}, {to_i(H),to_i(Min),to_i(S)}});

parse(Bin) when is_binary(Bin) ->
  binary_to_integer(Bin).



date_path(UTC) when is_integer(UTC) ->
  {Day,_} = date_time(UTC),
  date_path(Day);

date_path({Y,M,D}) ->
  iolist_to_binary(io_lib:format("~4..0B/~2..0B/~2..0B", [Y,M,D])).




daystart(UTCms) when is_integer(UTCms) ->
  % calendar:datetime_to_gregorian_seconds({Date, {0,0,0}})
  % DaystartMilliSeconds = UTCms - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})*1000,
  DayMS = timer:hours(24),
  (UTCms div DayMS)*DayMS.



% Convert milliseconds to seconds
utc(UnixTime) when is_integer(UnixTime), UnixTime > 4000000000 ->
  UnixTime div 1000;

% No convertion needed
utc(UTC) when is_integer(UTC) ->
  UTC;

% Convert given {Date, Time} or {Megasec, Sec, Microsec} to millisecond timestamp
utc({{_Y,_Mon,_D} = Day,{H,Min,S}}) ->
  GregSeconds_Zero = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds_Now = calendar:datetime_to_gregorian_seconds({Day,{H,Min,S}}),
  GregSeconds_Now - GregSeconds_Zero;

utc({Megaseconds, Seconds, _Microseconds}) ->
  Megaseconds*1000000 + Seconds.


date_time(Day) when length(Day) == 10 ->
  [Y,M,D] = string:tokens(Day, "-"),
  {{to_i(Y),to_i(M),to_i(D)},{0,0,0}};

date_time({Y,M,D}) when is_integer(Y),is_integer(M),is_integer(D) ->
  {{Y,M,D},{0,0,0}};

date_time(Timestamp) when is_number(Timestamp) ->
  GregSeconds_Zero = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds = GregSeconds_Zero + Timestamp,
  calendar:gregorian_seconds_to_datetime(GregSeconds).


