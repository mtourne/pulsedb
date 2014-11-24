-module(pulsedb_disk_milliseconds).
-include("pulsedb.hrl").
-behavior(pulsedb_disk).

-export([required_chunks/4, required_partitions/3]).
-export([chunk_number/2, tick_number/2]).
-export([block_path/1, parse_date/1]).
-export([block_start_utc/2, block_end_utc/2]).
-export([last_day/1]).


tick_number(TimestampMsec, #storage_config{ticks_per_chunk=NTicks}) ->
%%% reminder
%%%  UTC rem NTicks.
%%%  mtourne: it probably works that way but wasted space
    TimestampMsec.


chunk_number(TimestampMsec, #storage_config{ticks_per_chunk=NTicks, chunks_per_metric=NChunks}) ->
    ChunkNumber = (TimestampMsec rem (NChunks*NTicks)) div NTicks,
    lager:debug("Chunk #: ~p,TimestampMsec: ~p, Nchunks: ~p, Ntichks: ~p.",
                [ChunkNumber, TimestampMsec, NChunks, NTicks]),
    ChunkNumber.


block_start_utc(TimestampMsec, #storage_config{}) ->
    DayStart = pulsedb_time:daystart(TimestampMsec),
    lager:debug("DayStart: ~p.", [DayStart]),
    DayStart.


block_end_utc(TimestampMsec, #storage_config{ticks_per_chunk=NTicks, chunks_per_metric=NChunks}) ->
%%% XX (mtourne): check this.
    DayEnd = pulsedb_time:daystart({milliseconds, TimestampMsec}) + NChunks*NTicks - 1,
    lager:debug("DayEnd: ~p.", [DayEnd]),
    DayEnd.


%%% XX (mtourne): not sure what goes on here.
required_chunks(From, To, Date, #storage_config{ticks_per_chunk = NTicks, chunks_per_metric = NChunks}) ->
  F = From div NTicks,
  T = To div NTicks,
  DateStartD = Date div (NTicks * NChunks),

  [begin
     Offset = if
       F == H -> (From - H*NTicks) rem NTicks;
       true -> 0 end,

     Length0 = if
       T == F, T == H -> To - From + 1;
       T == H         -> To - H*NTicks + 1;
       true           -> NTicks end,

     % length correction
     Length = if
       Offset + Length0 > NTicks -> NTicks - Offset;
       true                      -> Length0 end,

     {H rem NChunks, Offset, Length}
     end || H <- lists:seq(F, T), H div NChunks == DateStartD].


%%% XX (mtourne): not sure what goes on here.
required_partitions(From, To, #storage_config{ticks_per_chunk = NTicks, chunks_per_metric = NChunks}) ->
  MetricTicksPerBlock = NTicks * NChunks,
  [block_path(X*MetricTicksPerBlock)
   || X <- lists:seq(From div MetricTicksPerBlock,To div MetricTicksPerBlock)].


block_path(TimestampMsec) ->
    pulsedb_time:date_path({milliseconds, TimestampMsec}).


parse_date(Date) ->
    lager:debug("Parse this date: ~p", [Date]),
    pulsedb_time:parse(Date).


last_folder(F, Path, Length) ->
  case prim_file:list_dir(F, Path) of
    {ok, List} ->
      case lists:reverse(lists:sort([Y || Y <- List, length(Y) == Length])) of
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
  (Day = last_folder(F, filename:join([Path,Year,Month]), 2)) =/= undefined orelse throw(undefined),
  filename:join([Path,Year,Month,Day]).
