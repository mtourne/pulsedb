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
  source_id :: non_neg_integer(),
  utc1 :: utc(),
  utc2 :: utc(),
  offset :: non_neg_integer(),
  size :: non_neg_integer()
}).

-type index_block() :: #index_block{}.



-record(db, {
  path :: file:filename(),
  sources :: [source()],
  index :: [{source_name(), index_block()}],

  config_fd_a :: file:fd(),
  data_fd_a :: file:fd(),
  index_fd_a :: file:fd(),

  config_fd_r :: file:fd(),
  data_fd_r :: file:fd(),
  index_fd_r :: file:fd(),
  date :: calendar:date() | undefined
}).


-type db() :: #db{}.


-type utc() :: non_neg_integer().


-record(tick, {
  name :: source_name(),
  utc :: utc(),
  value :: [{column_name(),value()}]
}).

-type tick() :: #tick{}.


