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

main(["migrate", "-f", Path]) -> 
  mig("all", true, [Path]);

main(["migrate", "-f", Mode|Rest]) 
when Mode == "all" 
orelse Mode == "shard" 
orelse Mode == "date" ->
  mig(Mode, true, Rest);


main(["migrate", Path]) -> 
  mig("all", false, [Path]);

main(["migrate", Mode|Rest]) 
when Mode == "all" 
orelse Mode == "shard" 
orelse Mode == "date" ->
  mig(Mode, false, Rest);


main(_) ->
  F = fun(Args, "") ->
          io:format("  ~s ~s~n", [?FILE, Args]);
         (Args, Help) ->
          io:format("  ~s ~s~n", [?FILE, Args]),
          io:format("  \t  ~s~n~n", [Help])
      end,
  io:format("~nDB migration script ~n"),
  io:format("-f flag means that migration will run even if minutes db already exists~n~n"),

  F("info <path>","Prints information about all the shards, dates and migrations found"),

  F("migrate [-f] <path>",""),
  F("migrate [-f] all <path>", "Migrates everything under <path>"),
  F("migrate [-f] shard <shard-path>", "Migrate all dates of <shard-path> (use it with unsharded db)"),
  F("migrate [-f] date <shard-path> <date>", "Migrate <date> of <shard-path> (use it with unsharded db)"),

  halt(1).





mig("all", Forced, [Path]) ->
  init(),
  Migrations = migrations(Path),
  print_migrations(Migrations),
  perform_migrations(Migrations, Forced),
  ok;

 
mig("shard", Forced, [Path]) ->
  case is_valid_shard(Path) of
    false -> 
      io:format("~s is not a valid db shard", [Path]),
      halt(1);
    true ->
      Migrations = shard_migrations(Path),
      print_migrations(Migrations),
      perform_migrations(Migrations, Forced),
      ok
  end;


mig("date", Forced, [Path, Date]) ->
  case is_valid_db(seconds, Path, Date) of
    false -> 
      io:format("~s is not a valid db", [Path]),
      halt(1);
    true ->
      Migrations = db_migrations(Path, Date),
      print_migrations(Migrations),
      perform_migrations(Migrations, Forced),
      ok
  end.









chunk_migrated({ok, Chunk}) ->
  io:format(" ~p ",[Chunk]);

chunk_migrated({error, Chunk, E}) ->
  io:format("~n  ~p\terror: ~p~n",[Chunk, E]).



migrations(Path) ->
  Shards0 = shards(Path),
  Shards1 = lists:flatmap(fun (Shard) -> shard_migrations(Shard) end, Shards0),
  Shards2 = lists:reverse(lists:keysort(2, Shards1)),
  lists:keysort(3, Shards2).

shard_migrations(Shard) ->
  Dates = dates(seconds, Shard),
  DBs0 = lists:flatmap(fun (Date) -> db_migrations(Shard, Date) end, Dates),
  DBs1 = lists:reverse(lists:keysort(2, DBs0)),
  lists:keysort(3, DBs1).

db_migrations(Shard, Date) ->
  [{Shard, Date, has_minutes(Shard, Date)}].




perform_migrations(Migrations) ->
  perform_migrations(Migrations, false).

perform_migrations(Migrations, Forced) ->
  ToMigrate = [{S,D} || {S,D,AlreadyDone} <- Migrations, Forced orelse (not AlreadyDone)],
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
  ok.


print_migrations(Migrations) ->
  io:format("migrations:~n"),
  [begin
     Status = if 
       HasM -> "done";
       true -> "TODO" 
     end,
     io:format("  [~s] ~s\t~s~n", [Status,S,D])
   end || {S,D,HasM} <- Migrations],
  ok.




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
