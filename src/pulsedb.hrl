-type source_name() :: binary().
-type value() :: integer().

-type db() :: term().


-type utc() :: non_neg_integer().

-type tick() :: {Name::binary(), UTC::utc(), Value::value(), [Tags::{atom(),binary()}]}.


-record(storage_config, 
 {
  ticks_per_chunk :: non_neg_integer(),
  tick_bytes = 2 :: non_neg_integer(),
  chunks_per_metric :: non_neg_integer(),
  chunk_bits :: non_neg_integer(),
  offset_bytes = 4 :: non_neg_integer(),
  utc_step :: non_neg_integer(),
  partition_module :: atom()
  }).

-type storage_config() :: #storage_config{}.
