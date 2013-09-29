-type source_name() :: binary().
-type column_name() :: binary().
-type value() :: integer().

-record(source, {
  source_id :: non_neg_integer(),
  name :: source_name(),
  columns = [] :: [column_name()],
  data_offset :: non_neg_integer(),
  data_offset_ptr :: non_neg_integer(),
  start_of_block :: non_neg_integer(),
  end_of_block :: non_neg_integer()
}).

-type source() :: #source{}.



-record(db, {
  path :: file:filename(),
  date :: binary() | undefined,
  sources :: [source()],

  mode :: undefined | read | append,

  config_fd :: file:fd(),
  data_fd :: file:fd()
}).


-type db() :: #db{}.


-type utc() :: non_neg_integer().
