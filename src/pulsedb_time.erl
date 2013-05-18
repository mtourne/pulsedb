-module(pulsedb_time).

-export([daystart/1]).


daystart(UTCms) when is_integer(UTCms) ->
  % calendar:datetime_to_gregorian_seconds({Date, {0,0,0}})
  % DaystartMilliSeconds = UTCms - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})*1000,
  DayMS = timer:hours(24),
  (UTCms div DayMS)*DayMS.
