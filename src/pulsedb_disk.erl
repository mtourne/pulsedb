-module(pulsedb_disk).
-author('Max Lapshin <max@maxidoors.ru>').

-include("pulsedb.hrl").
-include("../include/pulsedb.hrl").


-export([open/1, append/2, read/2, close/1]).
-export([info/1]).
% -export([write_events/3]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path) ->
  {ok, #db{path = Path}}.


open0(#db{path = Path, config_fd = undefined, date = Date} = DB, Mode) when Date =/= undefined ->
  case filelib:is_regular(filename:join([Path,Date,config_v2])) of
    true ->
      open_existing_db(DB#db{mode = Mode});
    false when Mode == read ->
      DB#db{mode = read};
    false when Mode == append ->
      create_new_db(DB#db{mode = append})
  end.


create_new_db(#db{path = Path, date = Date, mode = append} = DB) when Date =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v2]),
  DataPath = filename:join([Path, Date, data_v2]),

  case filelib:ensure_dir(ConfigPath) of
    ok -> ok;
    {error, Reason1} -> throw({error, {create_path_failed,ConfigPath,Reason1}})
  end,

  Opts = [binary,write,exclusive,raw],

  {ok, ConfigFd} = case file:open(ConfigPath, Opts) of
    {ok, CFile_} -> {ok, CFile_};
    {error, Reason2} -> throw({error,{open_config_failed,ConfigPath,Reason2}})
  end,

  {ok, DataFd} = file:open(DataPath, Opts),

  DB#db{path = Path, config_fd = ConfigFd, data_fd = DataFd, sources = []}.


read_file(Path) ->
  case file:read_file(Path) of
    {ok, Bin} -> Bin;
    {error, _} -> <<>>
  end.

open_existing_db(#db{path = Path, date = Date, mode = Mode} = DB) when Date =/= undefined, Mode =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v2]),
  DataPath = filename:join([Path, Date, data_v2]),

  DB1 = case DB#db.sources of
    undefined ->
      DB#db{sources = decode_config(read_file(ConfigPath))};
    _ ->
      DB
  end,

  Opts = case Mode of
    append -> [binary,read,write,raw];
    read -> [binary,read,raw]
  end,

  {ok, ConfigFd} = file:open(ConfigPath, Opts),
  {ok, DataFd} = file:open(DataPath, Opts),

  DB1#db{config_fd = ConfigFd, data_fd = DataFd}.





-spec append([pulsedb:tick()] | pulsedb:tick(), pulsedb:db()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
append(_, #db{mode = read}) ->
  {error, need_to_reopen_for_append};

append([], #db{} = DB) ->
  {ok, DB};

append([#tick{value = []}|Ticks], #db{} = DB) ->
  append(Ticks, DB);


append([#tick{utc = UTC}|_] = Ticks, #db{config_fd = undefined, date = undefined} = DB) ->
  append(Ticks, open0(DB#db{date = pulsedb_time:date_path(UTC)}, append));

append([#tick{}|_] = Ticks, #db{mode = append} = DB) ->
  append0(Ticks, DB).

append0([#tick{name = Name}=Tick|Ticks], #db{sources = S} = DB) ->
  {ok, DB1} = case lists:keyfind(Name, #source.name, S) of
    false -> append_new_source(Tick, DB);
    #source{} -> {ok, DB}
  end,
  {ok, DB2} = append_data(Tick, DB1),
  append0(Ticks, DB2);

append0([], #db{} = DB) ->
  {ok, DB}.



append_new_source(#tick{name = Name, value = Value}, #db{sources = Sources, config_fd = ConfigFd} = DB) ->
  Begin = case Sources of
    [] -> 0;
    _ -> 
      #source{end_of_block = E} = lists:last(Sources),
      E
  end,

  {ok, ConfigPos} = file:position(ConfigFd, eof),
  Columns = [Column || {Column,_} <- Value],
  EOF = Begin + 25*60*(4 + 8*length(Columns)),
  Source = #source{source_id = length(Sources), name = Name, columns = Columns, 
    start_of_block = Begin, end_of_block = EOF,
    data_offset = Begin, data_offset_ptr = ConfigPos + 3},
  Bin = encode_config(Source),
  ok = file:pwrite(ConfigFd, ConfigPos, Bin),
  {ok, DB#db{sources = Sources ++ [Source]}}.


-define(CONFIG_SOURCE, 1).

encode_config(#source{name = Name, columns = Columns, data_offset = Offset, start_of_block = Start, end_of_block = End}) ->
  L = size(Name),
  Count = length(Columns),
  Config = [begin
    Col = atom_to_binary(Column,latin1),
    <<(size(Col)), Col/binary>>
  end || Column <- Columns],
  Conf = [<<Offset:32, Start:32, End:32, L:16, Name:L/binary, Count>>,Config],
  Size = iolist_size(Conf),
  iolist_to_binary([<<?CONFIG_SOURCE, Size:16>>, Conf]).

-spec decode_config(binary()) -> [source()].

decode_config(Bin) when is_binary(Bin) ->
  decode_config(Bin, 0).

decode_config(<<?CONFIG_SOURCE, Length:16, Source:Length/binary, Rest/binary>>, ConfigOffset) ->
  <<Offset:32, Start:32, End:32, L:16, Name:L/binary, Count, Config/binary>> = Source,
  Columns = [binary_to_atom(Column,latin1) || <<C, Column:C/binary>> <= Config],
  Count = length(Columns),
  DataOffsetPtr = ConfigOffset + 3,
  [#source{name = Name, columns = Columns, start_of_block = Start, end_of_block = End, data_offset = Offset, data_offset_ptr = DataOffsetPtr}
    |decode_config(Rest, ConfigOffset + 1 + 2 + Length)];

decode_config(<<>>, _) ->
  [].





append_data(#tick{name = Name, utc = UTC, value = Values}, #db{data_fd = DataFd, config_fd = ConfigFd, sources = Sources} = DB) ->
  #source{data_offset = Offset, data_offset_ptr = ConfigPtr, columns = Columns, end_of_block = EOF} = Source =
    lists:keyfind(Name,#source.name,Sources),

  Block = [<<UTC:32>>,  [<<(proplists:get_value(Col,Values,0)):64>> || Col <- Columns ]],
  case iolist_size(Block) + Offset =< EOF of
    true ->
      file:pwrite(DataFd, Offset, Block),
      NewDataOffset = Offset + iolist_size(Block),
      file:pwrite(ConfigFd, ConfigPtr, <<NewDataOffset:32>>),
      Sources1 = lists:keystore(Name,#source.name, Sources, Source#source{data_offset = NewDataOffset}),
      {ok, DB#db{sources = Sources1}};
    false ->
      {ok, DB}
  end.





info(#db{sources = Sources}) when is_list(Sources) ->
  Src = [{Name,[{columns,Columns}]} || #source{name = Name, columns = Columns} <- Sources],
  [{sources,Src}];

info(#db{path = Path} = DB) ->
  case last_day(Path) of
    undefined -> [];
    DayPath -> info(DB#db{sources = decode_config(read_file(filename:join(DayPath,config_v2)))})
  end.


last_folder(F, Path, Length) ->
  case prim_file:list_dir(F, Path) of
    {ok, List} ->
      case lists:reverse(lists:sort([Y || Y <- List, length(Y) == Length])) of
        [] -> undefined;
        [Y|_] -> Y
      end;
    _ ->
      undefined
  end.

last_day(Path) ->
  {ok, F} = prim_file:start(),
  Val = try last_day0(F, Path)
  catch
    throw:_ -> undefined
  end,
  prim_file:stop(F),
  Val.

last_day0(F, Path) ->
  (Year = last_folder(F, Path, 4)) =/= undefined orelse throw(undefined),
  (Month = last_folder(F, filename:join(Path,Year), 2)) =/= undefined orelse throw(undefined),
  (Day = last_folder(F, filename:join([Path,Year,Month]), 2)) =/= undefined orelse throw(undefined),
  filename:join([Path,Year,Month,Day]).





-spec read(Query::[{atom(),any()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.

read(_Query, #db{mode = append}) ->
  {error, need_to_reopen_for_read};

read(Query, #db{config_fd = undefined, date = Date} = DB) when Date =/= undefined ->
  case open0(DB, read) of
    #db{config_fd = undefined} = DB1 ->
      {ok, [], DB1};
    #db{} = DB1 ->
      read(Query, DB1)
  end;

read(Query0, #db{sources = Sources, data_fd = DataFd, date = Date, mode = read} = DB) when Date =/= undefined ->
  Query = pulsedb:parse_query(Query0),
  {name, Name} = lists:keyfind(name, 1, Query),
  case lists:keyfind(Name, #source.name, Sources) of
    false ->
      {ok, [], DB};
    #source{start_of_block = Start, data_offset = Offset, columns = Columns} ->
      RowSize = 8*length(Columns),
      case file:pread(DataFd, Start, Offset - Start) of
        {ok, Bin} ->
          TicksBin1 = [ {UTC,Row} || <<UTC:32, Row:RowSize/binary>> <= Bin],
          TicksBin2 = filter_ticks(TicksBin1, proplists:get_value(from,Query), proplists:get_value(to,Query)),
          Ticks = [#tick{name = Name, utc = UTC, value = lists:zip(Columns, [V || <<V:64>> <= Row])} || {UTC,Row} <- TicksBin2],
          {ok, lists:keysort(#tick.utc, Ticks), DB};
        _ ->
          {ok, [], DB}
      end
  end.


filter_ticks([], _, _) ->
  [];

filter_ticks([{UTC,_}|Ticks], From, To) when From - UTC > 0 ->
  filter_ticks(Ticks, From, To);

filter_ticks([{UTC,_}|_Ticks], _From, To) when UTC - To >= 0 ->
  [];

filter_ticks([Tick|Ticks], From, To) ->
  [Tick|filter_ticks(Ticks, From, To)].




-spec close(pulsedb:db()) -> {pulsedb:db()}.
close(#db{config_fd = C, data_fd = D} = DB) ->
  file:close(C),
  file:close(D),
  {ok, DB#db{config_fd = undefined, data_fd = undefined, 
    date = undefined, sources = undefined}}.








