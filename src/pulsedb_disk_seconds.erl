-module(pulsedb_disk_seconds).
-include("pulsedb.hrl").
-behavior(pulsedb_disk).

-export([required_chunks/4, required_partitions/3]).
-export([chunk_number/2, tick_number/2]).
-export([block_path/1, parse_date/1]).
-export([block_start_utc/2, block_end_utc/2]).


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
     
     Length = if
       Offset > 0     -> NTicks - Offset;
       T == F, T == H -> To - From + 1;
       T == H         -> To - H*NTicks + 1;
       true           -> NTicks end,
     
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
