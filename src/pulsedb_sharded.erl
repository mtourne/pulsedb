-module(pulsedb_sharded).
-export([open/2, append/2, read/3, close/1, sync/1, info/1]).

-record(sharded_db, {
  storage = pulsedb_sharded,
  path :: file:filename(),
  options = [],
  partitions = [],
  partitions_append = [],
  shard_tag,
  tracker
}).

open(DBPath, Options) when is_list(DBPath) ->
  open(iolist_to_binary(DBPath), Options);
open(<<"sharded://", DBPath/binary>>, Options) ->
  open(DBPath, Options);
open(DBPath, Options) when is_binary(DBPath) ->
  lager:info("OPEN ~p", [DBPath]),
  ShardTag = proplists:get_value(shard_tag, Options, <<"account">>),
  Tracker = proplists:get_value(tracker, Options),
  Options1 = proplists:delete(sharded, Options),
  Options2 = proplists:delete(shard_tag, Options1),
  Options3 = proplists:delete(tracker, Options2),
  {ok, #sharded_db{path = DBPath, 
                   tracker = Tracker,
                   shard_tag = ShardTag, 
                   options = Options3}}.




read(Name, Query, #sharded_db{shard_tag = ShardTag} = DB) ->
  ShardName = proplists:get_value(ShardTag, Query),
  read0(ShardName, Name, Query, DB).


read0(ShardName, Name, Query, #sharded_db{partitions=Shards0, path=DBPath, options=Options} = State) when size(ShardName) > 0 ->
  case find_or_open(ShardName, Shards0, DBPath, Options) of
    {ok, Shard0} -> 
      case pulsedb:read(Name, Query, Shard0) of
        {ok, Ticks, Shard1} -> {ok, Ticks, close(update_shards(ShardName, Shard1, State))};
        {error, Error}   -> Error
      end;
    {error, Error} -> 
      Error
  end;

read0(undefined, Name, Query, #sharded_db{path=DBPath, options=Options} = State) ->
  Shards = filelib:wildcard("*", binary_to_list(DBPath)),
  Partitions0 = [open_partition(Shard, DBPath, Options) || Shard <- Shards],
  Partitions = proplists:get_all_values(ok, Partitions0),
  Data0 = [pulsedb:read(Name, Query, Partition) || Partition <- Partitions],
  Data1 = [Part || {ok, Part, _} <- Data0],
  Data = lists:sort(lists:concat(Data1)),
  Ticks1 = pulsedb_data:aggregate(proplists:get_value(aggregator,Query), Data),
  Ticks2 = pulsedb_data:downsample(proplists:get_value(downsampler,Query), Ticks1),
  {ok, Ticks2, State}.
  
find_or_open(ShardName, Shards, Path, Options) when size(ShardName) > 0 ->
  case lists:keyfind(ShardName, 1, Shards) of
    false -> 
      open_partition(ShardName, Path, Options);
    {ShardName, Shard} -> 
      {ok, Shard}
  end.

open_partition(ShardName, DBPath, Options) ->
  ShardPath = partition_path(DBPath, ShardName),
  pulsedb:open(ShardPath, Options).
  

update_shards(ShardName, Shard, #sharded_db{partitions=Shards0} = State) ->
  Shards1 = lists:keystore(ShardName, 1, Shards0, {ShardName, Shard}),
  State#sharded_db{partitions=Shards1}.




close(#sharded_db{partitions=Shards0} = DB) ->
  [pulsedb:close(D) || {_, D} <- Shards0],
  DB#sharded_db{partitions = []}.



append(Ticks, #sharded_db{} = State) when is_list(Ticks) ->
  State1 = lists:foldl(fun (Tick, St) -> 
                         {ok, St1} = append(Tick, St), 
                         St1 
                       end, State, Ticks),
  {ok, State1};


append({Name,UTC,Value,Tags}, #sharded_db{tracker=Tracker, shard_tag = ShardTag,
                                          path=DBPath, partitions_append=Partitions0} = State) ->
  ShardName = proplists:get_value(ShardTag, Tags),
  {DB, Partitions1} = 
  case proplists:get_value(ShardName, Partitions0) of
    undefined ->
      RealPath = partition_path(DBPath, ShardName),
      Spec = {ShardName, {pulsedb_worker, start_link, [undefined, [{url,RealPath},{timeout,120*1000}]]}, temporary, 200, worker, []},
      {ok, DB_} = gen_tracker:find_or_open(Tracker, Spec),
      {DB_, [Partitions0]};
    DB_ ->
      {DB_, proplists:delete(ShardName, Partitions0)}
  end,
  
  Partitions2 = 
  case pulsedb:append({Name,UTC,Value,Tags}, DB) of
    {ok, DB1} -> [{ShardName, DB1}|Partitions1];
    undefined -> [{ShardName, DB}|Partitions1]
  end,
  
  {ok, State#sharded_db{partitions_append = Partitions2}}.


info(#sharded_db{path=DBPath, options=Options}) ->
  Shards = filelib:wildcard("*", binary_to_list(DBPath)),
  Partitions0 = [open_partition(Shard, DBPath, Options) || Shard <- Shards],
  Partitions = proplists:get_all_values(ok, Partitions0),
  Infos = lists:concat([pulsedb:info(Partition) || Partition <- Partitions]),
  Sources = lists:concat(proplists:get_all_values(sources, Infos)),
  [{sources, lists:usort(Sources)}].


sync(#sharded_db{partitions_append=Partitions}) ->
  [pulsedb:sync(DB) || {_, DB} <- Partitions].


partition_path(DBPath, Partition) ->
  iolist_to_binary(["file://", DBPath, "/", Partition]).
