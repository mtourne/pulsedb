-module(pulsedb_sharded).
-export([open/2, append/2, read/3, close/1, sync/1]).

-record(sharded_db, {
  storage = pulsedb_sharded,
  path :: file:filename(),
  partitions = [],
  shard_tag
}).

open(DBPath, Options) when is_list(DBPath) ->
  open(iolist_to_binary(DBPath), Options);
open(<<"sharded://", DBPath/binary>>, Options) ->
  open(DBPath, Options);
open(DBPath, Options) when is_binary(DBPath) ->
  lager:info("OPEN ~p", [DBPath]),
  ShardTag = proplists:get_value(shard_tag, Options, <<"account">>),
  {ok, #sharded_db{path = DBPath, 
                   shard_tag = ShardTag}}.




read(Name, Query, #sharded_db{shard_tag = ShardTag} = DB) ->
  ShardName = proplists:get_value(ShardTag, Query),
  read0(ShardName, Name, Query, DB).


read0(ShardName, Name, Query, #sharded_db{partitions=Shards0, path=DBPath} = State) when size(ShardName) > 0 ->
  case find_or_open(ShardName, Shards0, DBPath) of
    {ok, Shard0} -> 
      case pulsedb:read(Name, Query, Shard0) of
        {ok, Ticks, Shard1} -> {ok, Ticks, close(update_shards(ShardName, Shard1, State))};
        {error, Error}   -> Error
      end;
    {error, Error} -> 
      Error
  end;

read0(undefined, _, _, #sharded_db{} = State) ->
  {ok, [], State}.
  
find_or_open(ShardName, Shards, Path) when size(ShardName) > 0 ->
  case lists:keyfind(ShardName, 1, Shards) of
    false -> 
      open_partition(ShardName, Path);
    {ShardName, Shard} -> 
      {ok, Shard}
  end.

open_partition(ShardName, DBPath) ->
  ShardPath = iolist_to_binary(["file://", DBPath, "/", ShardName]),
  pulsedb:open(ShardPath).
  

update_shards(ShardName, Shard, #sharded_db{partitions=Shards0} = State) ->
  Shards1 = lists:keystore(ShardName, 1, Shards0, {ShardName, Shard}),
  State#sharded_db{partitions=Shards1}.




close(#sharded_db{partitions=Shards0} = DB) ->
  [pulsedb:close(D) || {_, D} <- Shards0],
  DB#sharded_db{partitions = []}.


append(_, #sharded_db{}) ->
  {error, not_impl}.

sync(#sharded_db{}) ->
  {error, not_impl}.
