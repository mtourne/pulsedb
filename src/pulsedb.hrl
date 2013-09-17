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
  date :: binary() | undefined,
  sources :: [source()],
  index :: [{source_name(), index_block()}],

  mode :: undefined | read | append,

  config_fd :: file:fd(),
  data_fd :: file:fd(),
  index_fd :: file:fd()
}).


-type db() :: #db{}.


-type utc() :: non_neg_integer().
