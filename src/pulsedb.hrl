-type source_name() :: binary().
-type value() :: integer().

-type db() :: term().


-type utc() :: non_neg_integer().

-type tick() :: {Name::binary(), UTC::utc(), Value::value(), [Tags::{atom(),binary()}]}.

