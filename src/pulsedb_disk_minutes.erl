-module(pulsedb_disk_minutes).
-include("pulsedb.hrl").
-behavior(pulsedb_disk).

-export([required_chunks/4, required_partitions/3]).
-export([chunk_number/2, tick_number/2]).
-export([block_path/1, parse_date/1]).
-export([block_start_utc/2, block_end_utc/2]).
-export([last_day/1]).

%%%%%%%%%%%%%%%%%%%%%%
% INTERFACE FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%
tick_number(UTC, #storage_config{ticks_per_chunk=NTicks}) ->
  (UTC div 60) rem NTicks.


chunk_number(UTC, #storage_config{}) ->
  {{_,_,D},_} = pulsedb_time:date_time(UTC),
  D - 1.


block_start_utc(UTC, #storage_config{}) ->
  {{Y,M,_},_} = pulsedb_time:date_time(UTC),
  date_utc({Y,M,1}).

block_end_utc(UTC, #storage_config{}) ->
  NextMonth = month_num(UTC) + 1,
  Date = month_num_date(NextMonth),
  date_utc(Date) - 1.
  
  
required_chunks(From, To, Date, #storage_config{ticks_per_chunk = NTicks, utc_step = Step}) ->
  F = chunk_start_utc(From) div (NTicks*Step),
  T = chunk_start_utc(To) div (NTicks*Step),
  DateMonth = month_num(Date),
  
  [begin
     {{_,_,D},_} = pulsedb_time:date_time(DayN*NTicks*Step),
     Chunk = D - 1,
     
     Offset = if 
       F == DayN -> ((From div Step) - DayN*NTicks) rem NTicks;
       true -> 0 end,
     
     Length0 = if
       T == F, T == DayN -> (To div Step) - (From div Step) + 1;
       T == DayN         -> (To div Step) - DayN*NTicks + 1;
       true              -> NTicks end,
     
     % length correction
     Length = if 
       Offset + Length0 > NTicks -> NTicks - Offset;
       true                      -> Length0 end,
     
     {Chunk, Offset, Length}
     end || DayN <- lists:seq(F, T), month_num(DayN*NTicks*Step) == DateMonth].



required_partitions(From, To, #storage_config{}) ->
  [month_path(month_num_date(X)) || X <- lists:seq(month_num(From),month_num(To))].


block_path(UTC) ->
  month_path(UTC).


parse_date(<<YM:7/binary, Sep:1/binary, _/binary>>) ->
  pulsedb_time:parse(<<YM/binary, Sep:1/binary, "01">>).




month_path(UTC) when is_integer(UTC) ->
  {Day,_} = pulsedb_time:date_time(UTC),
  month_path(Day);

month_path({Y,M,_}) ->
  iolist_to_binary(io_lib:format("~4..0B/~2..0B/minutes", [Y,M])).


%%%%%%%%%%%%%%%%%
% DATETIME UTILS
%%%%%%%%%%%%%%%%%

month_num(UTC) ->
  {{Y,M,_},{_,_,_}} = pulsedb_time:date_time(UTC),
  Y * 12 + M - 1.
  
month_num_date(Num) when is_number(Num) ->
  Y = Num div 12, 
  M =(Num rem 12) + 1,
  {Y,M,1}.

date_utc({Y,M,D}) when is_integer(Y),is_integer(M),is_integer(D) ->
   pulsedb_time:utc({{Y,M,D},{0,0,0}}).

chunk_start_utc(UTC) ->
  {Date,_} = pulsedb_time:date_time(UTC),
  date_utc(Date).



last_folder(F, Path, Length) when is_number(Length) ->
  case prim_file:list_dir(F, Path) of
    {ok, List} ->
      case lists:reverse(lists:sort([Y || Y <- List, length(Y) == Length])) of
        [] -> undefined;
        [Y|_] -> Y
      end;
    _ ->
      undefined
  end;

last_folder(F, Path, Name) ->
  case prim_file:list_dir(F, Path) of
    {ok, List} ->
      case [Y || Y <- List, Y == Name] of
        [] -> undefined;
        [Y|_] -> Y
      end;
    _ ->
      undefined
  end.


last_day(Path) ->
  {ok, F} = prim_file:start(),
  Val = try last_day0(F, Path)
  catch
    throw:_ -> undefined
  end,
  prim_file:stop(F),
  Val.

last_day0(F, Path) ->
  (Year = last_folder(F, Path, 4)) =/= undefined orelse throw(undefined),
  (Month = last_folder(F, filename:join(Path,Year), 2)) =/= undefined orelse throw(undefined),
  (Day = last_folder(F, filename:join([Path,Year,Month]), "minutes")) =/= undefined orelse throw(undefined),
  filename:join([Path,Year,Month,Day]).
