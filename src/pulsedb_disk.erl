-module(pulsedb_disk).
-author('Max Lapshin <max@maxidoors.ru>').

-include("pulsedb.hrl").


-export([open/1, append/2, read/2, close/1]).
% -export([write_events/3]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path) ->
  {ok, #db{path = Path}}.


open0(#db{path = Path, config_fd = undefined} = DB, Mode) ->
  case filelib:is_regular(filename:join(Path,config_v1)) of
    true ->
      open_existing_db(DB#db{mode = Mode});
    false ->
      create_new_db(DB)
  end.



create_new_db(#db{path = Path} = DB) ->
  ConfigPath = filename:join(Path, config_v1),
  IndexPath = filename:join(Path, index_v1),
  DataPath = filename:join(Path, data_v1),

  case filelib:ensure_dir(ConfigPath) of
    ok -> ok;
    {error, Reason1} -> throw({error, {create_path_failed,ConfigPath,Reason1}})
  end,

  Opts = [binary,append,exclusive,raw],

  {ok, ConfigFd} = case file:open(ConfigPath, Opts) of
    {ok, CFile_} -> {ok, CFile_};
    {error, Reason2} -> throw({error,{open_config_failed,ConfigPath,Reason2}})
  end,

  {ok, IndexFd} = file:open(IndexPath, Opts),
  {ok, DataFd} = file:open(DataPath, Opts),

  DB#db{path = Path, config_fd = ConfigFd, index_fd = IndexFd, data_fd = DataFd}.


read_file(Path) ->
  case file:read_file(Path) of
    {ok, Bin} -> Bin;
    {error, _} ->
      {ok, F} = file:open(Path, [binary,write,exclusive,raw]),
      file:close(F),
      <<>>
  end.

open_existing_db(#db{mode = Mode, path = Path} = DB) ->
  ConfigPath = filename:join(Path, config_v1),
  IndexPath = filename:join(Path, index_v1),
  DataPath = filename:join(Path, data_v1),

  Sources = pulsedb_format:decode_config(read_file(ConfigPath)),

  Opts = case Mode of
    append -> [binary,append,raw];
    read -> [binary,read,raw]
  end,

  {ok, ConfigFd} = file:open(ConfigPath, Opts),

  RawIndex = pulsedb_format:decode_index(read_file(IndexPath)),
  Index = unpack_index(Sources, RawIndex),
  {ok, IndexFd} = file:open(IndexPath, Opts),
  {ok, DataFd} = file:open(DataPath, Opts),
  DB#db{mode = Mode, path = Path, config_fd = ConfigFd, index_fd = IndexFd, data_fd = DataFd, sources = Sources, index = Index}.



-spec append([pulsedb:tick()], pulsedb:db()) -> {ok, pulsedb:db()}.
append([], #db{} = DB) ->
  {ok, DB};

append([#tick{value = []}|_], #db{} = DB) ->
  {ok, DB};

append(Ticks, #db{config_fd = undefined} = DB) ->
  append(Ticks, open0(DB, append));

append([#tick{name = Name, value = Value}|_] = Ticks, #db{} = DB) ->
  validate_ticks(Ticks),
  {ok, DB1} = append_config_if_required(Name, [Column || {Column,_} <- Value], DB),
  {ok, IndexBlock, DB2} = append_data(Ticks, DB1),
  {ok, DB3} = append_index(Name, IndexBlock, DB2),
  {ok, DB3}.

append_config_if_required(Name, Columns, #db{sources = Sources, config_fd = ConfigFd} = DB) ->
  case lists:keyfind(Name, #source.name, Sources) of
    #source{} -> 
      {ok, DB};
    false ->
      Source = #source{source_id = length(Sources), name = Name, columns = Columns},
      Bin = pulsedb_format:encode_config(Source),
      ok = file:write(ConfigFd, Bin),
      ok = file:sync(ConfigFd),
      {ok, DB#db{sources = Sources ++ [Source]}}
  end.


append_data([#tick{name = Name}|_] = Ticks, #db{data_fd = DataFd, sources = Sources} = DB1) ->
  UTC1 = (hd(Ticks))#tick.utc,
  UTC2 = (lists:last(Ticks))#tick.utc,
  {ok, Offset} = file:position(DataFd, cur),
  #source{source_id = SourceId} = Source = lists:keyfind(Name,#source.name,Sources),
  Bin = pulsedb_format:encode_data(Source, Ticks),
  ok = file:write(DataFd, Bin),
  ok = file:sync(DataFd),
  {ok, #index_block{source_id = SourceId, utc1 = UTC1,utc2 = UTC2,offset = Offset,size = iolist_size(Bin)}, DB1}.

append_index(Name, #index_block{} = IndexBlock, #db{index_fd = IndexFd, index = Index} = DB1) ->
  ok = file:write(IndexFd, pulsedb_format:encode_index(IndexBlock)),
  ok = file:sync(IndexFd),
  Index1 = case lists:keyfind(Name, 1, Index) of
    false ->
      [{Name,[IndexBlock]}|Index];
    {Name, IndexBlockList} ->
      lists:keystore(Name, 1, Index, {Name,IndexBlockList ++ [IndexBlock]})
  end,
  {ok, DB1#db{index = Index1}}.





validate_ticks([#tick{name = Name, utc = UTC}|Ticks]) ->
  validate_ticks(Ticks, Name, UTC).

validate_ticks([],_,_) -> ok;
validate_ticks([#tick{name = Name, utc = UTC1}|Ticks], Name, UTC) when UTC1 > UTC -> validate_ticks(Ticks, Name, UTC1);
validate_ticks([Tick|_], Name, UTC) -> error({wrong_tick,Name,UTC,Tick}).







-spec read(Query::[{atom(),any()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()}.
read(Query, #db{config_fd = undefined} = DB) ->
  read(Query, open0(DB, read));

read(Query0, #db{index = Index, sources = Sources, data_fd = DataFd} = DB) ->
  Query = pulsedb:parse_query(Query0),
  {name, Name} = lists:keyfind(name, 1, Query),
  case lists:keyfind(Name, 1, Index) of
    false ->
      {ok, [], DB};
    {Name,IndexBlocks1} ->
      IndexBlocks2 = filter_index_blocks(Query, IndexBlocks1),
      #source{} = Source = lists:keyfind(Name, #source.name, Sources),
      Ticks = lists:flatmap(fun(#index_block{offset = Offset, size = Size}) ->
        {ok, Bin} = file:pread(DataFd, Offset, Size),
        pulsedb_format:decode_data(Source, Bin)
      end, IndexBlocks2),
      {ok, Ticks, DB}
  end.



filter_index_blocks([], Blocks) -> Blocks;
filter_index_blocks(_, []) -> [];
filter_index_blocks([{from,From}|Query], Blocks) ->
  Blocks1 = lists:dropwhile(fun(#index_block{utc2 = UTC2}) -> UTC2 < From end, Blocks),
  filter_index_blocks(Query, Blocks1);
filter_index_blocks([{to,To}|Query], Blocks) ->
  Blocks1 = lists:takewhile(fun(#index_block{utc1 = UTC1}) -> UTC1 =< To end, Blocks),
  filter_index_blocks(Query, Blocks1);
filter_index_blocks([_|Query], Blocks) ->
  filter_index_blocks(Query, Blocks).



unpack_index([], []) -> [];
unpack_index(_, []) -> [];
unpack_index([#source{source_id = Id, name = Name}|Sources], Index) ->
  {SourceIndex, Rest} = lists:partition(fun(#index_block{source_id = I}) -> Id == I end, Index),
  [{Name,SourceIndex}|unpack_index(Sources, Rest)].




-spec close(pulsedb:db()) -> ok.
close(#db{config_fd = ConfigFd, index_fd = IndexFd, data_fd = DataFd}) ->
  file:close(ConfigFd),
  file:close(IndexFd),
  file:close(DataFd),
  ok.








