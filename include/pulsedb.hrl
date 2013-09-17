


-record(tick, {
  name :: pulsedb:source_name(),
  utc :: pulsedb:utc(),
  value :: [{pulsedb:column_name(),pulsedb:value()}]
}).

-type tick() :: #tick{}.



