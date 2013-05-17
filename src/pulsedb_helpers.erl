-module(pulsedb_helpers).
-include("../include/pulsedb.hrl").
-include("pulsedb.hrl").
-include("log.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([timestamp/1]).


% Convert seconds to milliseconds
timestamp(UnixTime) when is_integer(UnixTime), UnixTime < 4000000000 ->
  UnixTime * 1000;

% No convertion needed
timestamp(UTC) when is_integer(UTC) ->
  UTC;

% Convert given {Date, Time} or {Megasec, Sec, Microsec} to millisecond timestamp
timestamp({{_Y,_Mon,_D} = Day,{H,Min,S}}) ->
  timestamp({Day, {H,Min,S, 0}});

timestamp({{_Y,_Mon,_D} = Day,{H,Min,S, Milli}}) ->
  GregSeconds_Zero = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds_Now = calendar:datetime_to_gregorian_seconds({Day,{H,Min,S}}),
  (GregSeconds_Now - GregSeconds_Zero)*1000 + Milli;

timestamp({Megaseconds, Seconds, Microseconds}) ->
  (Megaseconds*1000000 + Seconds)*1000 + Microseconds div 1000.


date_time(Timestamp) ->
  GregSeconds_Zero = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  GregSeconds = GregSeconds_Zero + Timestamp div 1000,
  calendar:gregorian_seconds_to_datetime(GregSeconds).

