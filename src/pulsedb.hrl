-type source_name() :: binary().
-type column_name() :: binary().
-type value() :: integer().

-record(source, {
  source_id :: non_neg_integer(),
  name :: source_name(),
  columns = [] :: [column_name()]
}).

-type source() :: #source{}.


-record(index_block, {
  utc1 :: utc(),
  utc2 :: utc(),
  offset :: non_neg_integer(),
  size :: non_neg_integer()
}).

-type index_block() :: #index_block{}.



-record(db, {
  path :: file:filename(),
  config_fd :: file:fd(),
  data_fd :: file:fd(),
  index_fd :: file:fd(),
  mode :: read | append,
  date :: calendar:date() | undefined,
  sources = [] :: [source()],
  index = [] :: [{source_name(), index_block()}]
}).


-type db() :: #db{}.


-type utc() :: non_neg_integer().


-record(tick, {
  name :: source_name(),
  utc :: utc(),
  value :: [{column_name(),value()}]
}).

-type tick() :: #tick{}.


