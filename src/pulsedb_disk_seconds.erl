-module(pulsedb_disk_seconds).
-include("pulsedb.hrl").
-behavior(pulsedb_disk).

-export([required_chunks/4, required_partitions/3]).
-export([chunk_number/2, tick_number/2]).
-export([block_path/1, parse_date/1]).
-export([block_start_utc/2, block_end_utc/2]).
-export([last_day/1]).


tick_number(UTC, #storage_config{ticks_per_chunk=NTicks}) ->
  UTC rem NTicks.


chunk_number(UTC, #storage_config{ticks_per_chunk=NTicks, chunks_per_metric=NChunks}) ->
  (UTC rem (NChunks*NTicks)) div NTicks.


block_start_utc(UTC, #storage_config{}) ->
  pulsedb_time:daystart(UTC).

block_end_utc(UTC, #storage_config{ticks_per_chunk=NTicks, chunks_per_metric=NChunks}) ->
  pulsedb_time:daystart(UTC)+NChunks*NTicks - 1.
  

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


required_partitions(From, To, #storage_config{ticks_per_chunk = NTicks, chunks_per_metric = NChunks}) ->
  MetricTicksPerBlock = NTicks * NChunks,
  [block_path(X*MetricTicksPerBlock) 
   || X <- lists:seq(From div MetricTicksPerBlock,To div MetricTicksPerBlock)].


block_path(UTC) ->
  pulsedb_time:date_path(UTC).


parse_date(Date) ->
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
