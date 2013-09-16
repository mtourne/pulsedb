-type source_name() :: binary().
-type column_name() :: binary().
-type value() :: integer().

-record(source, {
  source_id :: non_neg_integer(),
  name :: source_name(),
  columns = [] :: [column_name()]
}).

-type source() :: #source{}.

-record(db, {
  path :: file:filename(),
  config_fd :: file:fd(),
  data_fd :: file:fd(),
  index_fd :: file:fd(),
  sources = [] :: [source()]
}).


-type db() :: #db{}.


-type utc() :: non_neg_integer().


-record(tick, {
  name :: source_name(),
  utc :: utc(),
  value :: [{column_name(),value()}]
}).

-type tick() :: #tick{}.
