#!/usr/bin/env escript
%%
%%! -env ERL_LIBS deps:.

init() ->
  application:load(lager),
  application:start(lager).


main(["info", Path]) ->
  init(),
  io:format("shards:~n"),
  Shards = shards(Path),
  [io:format("  ~s~n", [Shard]) || Shard <- Shards],
  io:format("~n"),

  [try
     io:format("shard ~s:~n", [Shard]),

     io:format("  seconds data:~n"),
     Seconds = dates(seconds, Shard),
     [io:format("    ~s~n", [DB]) || DB <- Seconds],

     io:format("  minutes data:~n"),
     Minutes = dates(minutes, Shard),
     [io:format("    ~s~n", [DB]) || DB <- Minutes],

     io:format("~n")
   catch C:E -> io:format("[Error] ~p~n", [{C,E}])
   end || Shard <- Shards],
  
  Migrations = migrations(Path),
  print_migrations(Migrations),
  ok;


main(["migrate", Path]) ->
  main(["migrate", "all", Path]);


main(["migrate", "all", Path]) ->
  init(),
  Migrations = migrations(Path),
  print_migrations(Migrations),
  
  ToMigrate = [{S,D} || {S,D,false} <- Migrations],
  io:format("~n"),
  _ = 
  [try
     io:format("migration ~s ~s~n", [Shard, Date]),
     pulsedb_aggregator:migrate_day(minutes, Shard, Date, fun (R) -> chunk_migrated(R) end),
     io:format("~n")
   catch C:E -> 
     io:format("[Error] ~p~n", [{C,E}])
   end || {Shard, Date} <- ToMigrate],
  
  io:format("done~n"),
  ok;

 
main(["migrate", "shard", _Path]) ->
  io:format("not implemented~n"),
  halt(1);

main(["migrate", "date", _Path]) ->
  io:format("not implemented~n"),
  halt(1);


main(_) ->
  io:format("~s [info|migrate] [all|shard|date] <path>~n", [escript:script_name()]),
  halt(1).


chunk_migrated({ok, Chunk}) ->
  io:format("  ~p\tok~n",[Chunk]);

chunk_migrated({error, Chunk, E}) ->
  io:format("  ~p\terror: ~p~n",[Chunk, E]).



migrations(Path) ->
  Shards0 = shards(Path),
  Shards1 = lists:flatmap(fun (Shard) ->
    Dates = dates(seconds, Shard),
    [{Shard, Date, has_minutes(Shard, Date)} || Date <- Dates]
  end, Shards0),

  Shards2 = lists:reverse(lists:keysort(2, Shards1)),
  lists:keysort(3, Shards2).

print_migrations(Migrations) ->
  io:format("migrations:~n"),
  [begin
     Status = if 
       HasM -> "done";
       true -> "TODO" 
     end,
     io:format("  [~s] ~s\t~s~n", [Status,S,D])
   end || {S,D,HasM} <- Migrations],
  io:format("~n").




shards(Path) ->
  Dirs = [filename:join(Path, D) || D <- filelib:wildcard("*", Path)],
  [Dir || Dir <- Dirs, is_valid_shard(Dir)].



dates(Resolution, ShardPath) ->
  Dirs0 = filelib:wildcard("*/*/*", ShardPath),
  Dirs1 = lists:sort(Dirs0),
  Dirs = lists:reverse(Dirs1),
  [Dir || Dir <- Dirs, is_valid_db(Resolution, ShardPath, Dir)].
  




is_valid_shard(Path) ->
  filelib:is_dir(Path).
  
  
is_valid_db(seconds, Path, Date) ->
  DBPath = filename:join(Path, Date),
  Conf = filename:join(DBPath, config_v4),
  Data = filename:join(DBPath, data_v4),
  
  match == re:run(Date, "^[0-9]{4}/[0-9]{2}/[0-9]{2}$", [{capture, none}])
    andalso filelib:is_dir(DBPath)
    andalso filelib:is_file(Conf)
    andalso filelib:is_file(Data)
    andalso (not filelib:is_dir(Conf))
    andalso (not filelib:is_dir(Data));
  

is_valid_db(minutes, Path, Date) ->
  DBPath = filename:join(Path, Date),
  Conf = filename:join(DBPath, config_v4),
  Data = filename:join(DBPath, data_v4),
  
  match == re:run(Date, "^[0-9]{4}/[0-9]{2}/minutes$", [{capture, none}])
    andalso filelib:is_dir(DBPath)
    andalso filelib:is_file(Conf)
    andalso filelib:is_file(Data)
    andalso (not filelib:is_dir(Conf))
    andalso (not filelib:is_dir(Data)).


has_minutes(Path, Date) ->
  DateUTC = pulsedb_disk_minutes:parse_date(Date),
  MinutesDate = pulsedb_disk_minutes:block_path(DateUTC),
  
  is_valid_db(seconds, Path, Date)
    andalso is_valid_db(minutes, Path, MinutesDate).
