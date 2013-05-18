-module(pulsedb_appender).
-author('Max Lapshin <max@maxidoors.ru>').

-include("../include/pulsedb.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("pulsedb.hrl").
-include("log.hrl").


-export([open/1, open/2, append/2, close/1]).
-export([write_events/3]).


open(Path) ->
  open(Path, []).

open(Path, Opts) ->
  case filelib:is_regular(Path) of
    true ->
      open_existing_db(Path, Opts);
    false ->
      create_new_db(Path, Opts)
  end.


close(#dbstate{file = File} = _State) ->
  file:close(File),
  ok.


write_events(Path, Events, Options) ->
  {ok, S0} = pulsedb_appender:open(Path, Options),
  S1 = lists:foldl(fun(Event, State) ->
        {ok, NextState} = pulsedb_appender:append(Event, State),
        NextState
    end, S0, Events),
  ok = pulsedb_appender:close(S1).




%% @doc Here we create skeleton for new DB
%% Structure of file is following:
%% #!/usr/bin/env pulsedb
%% header: value
%% header: value
%% header: value
%%
%% chunkmap of fixed size
%% rows
create_new_db(Path, Opts) ->
  filelib:ensure_dir(Path),
  {ok, File} = file:open(Path, [binary,write,exclusive,raw]),
  {ok, 0} = file:position(File, bof),
  ok = file:truncate(File),

  State = #dbstate{
    mode = append,
    version = ?PULSEDB_VERSION,
    sync = not lists:member(nosync, Opts),
    path = Path,
    chunk_size = proplists:get_value(chunk_size, Opts, 5*60)
  },

  {ok, ChunkMapOffset} = write_header(File, State),
  {ok, _CMSize} = write_chunk_map(File, State),

  {ok, State#dbstate{
      file = File,
      chunk_map_offset = ChunkMapOffset
    }}.


open_existing_db(Path, _Opts) ->
  pulsedb_reader:open_existing_db(Path, [binary,write,read,raw]).


% Validate event and return {Type, Timestamp} if valid
validate_event({row, TS, Values} = Event) ->
  valid_values(Values) orelse erlang:throw({?MODULE, bad_values, Event}),
  is_integer(TS) andalso TS > 0 orelse erlang:throw({?MODULE, bad_timestamp, Event}),
  ok;

validate_event(Event) ->
  erlang:throw({?MODULE, invalid_event, Event}).


valid_values([V|Values]) when is_integer(V) -> valid_values(Values);
valid_values([]) -> true;
valid_values(_) -> false.


append(_Event, #dbstate{mode = Mode}) when Mode =/= append ->
  {error, reopen_in_append_mode};

append({row, TS, _Values} = Event, #dbstate{next_chunk_time = NCT, file = File, last_row = LastRow, sync = Sync} = State) ->
  validate_event(Event),
  if
    (TS >= NCT orelse NCT == undefined) ->
      {ok, EOF} = file:position(File, eof),
      {ok, State_} = append_full_row(Event, State),
      if Sync -> file:sync(File); true -> ok end,
      {ok, State1_} = start_chunk(TS, EOF, State_),
      if Sync -> file:sync(File); true -> ok end,
      {ok, State1_};
    LastRow == undefined ->
      append_full_row(Event, State);
    true ->
      append_delta_row(Event, State)
  end.


write_header(File, #dbstate{chunk_size = CS, version = Version}) ->
  Opts = [{chunk_size,CS},{version,Version}],
  {ok, 0} = file:position(File, 0),
  ok = file:write(File, <<"#!/usr/bin/env pulsedb\n">>),
  lists:foreach(fun
    ({Key, Value}) ->
      ok = file:write(File, [io_lib:print(Key), ": ", pulsedb_format:format_header_value(Key, Value), "\n"])
    end, Opts),
  ok = file:write(File, "\n"),
  file:position(File, cur).


write_chunk_map(File, #dbstate{chunk_size = ChunkSize}) ->
  ChunkCount = ?NUMBER_OF_CHUNKS(ChunkSize),

  ChunkMap = [<<0:?OFFSETLEN>> || _ <- lists:seq(1, ChunkCount)],
  Size = ?OFFSETLEN * ChunkCount,

  ok = file:write(File, ChunkMap),
  {ok, Size}.



start_chunk(Timestamp, Offset, #dbstate{daystart = undefined} = State) ->
  start_chunk(Timestamp, Offset, State#dbstate{daystart = pulsedb_time:daystart(Timestamp)});

start_chunk(Timestamp, Offset, #dbstate{daystart = Daystart, chunk_size = ChunkSize,
    chunk_map = ChunkMap} = State) ->

  ChunkSizeMs = timer:seconds(ChunkSize),
  ChunkNumber = (Timestamp - Daystart) div ChunkSizeMs,

  % sanity check
  (Timestamp - Daystart) < timer:hours(24) orelse erlang:error({not_this_day, Timestamp, Timestamp - Daystart}),

  ChunkOffset = current_chunk_offset(Offset, State),
  write_chunk_offset(ChunkNumber, ChunkOffset, State),

  NextChunkTime = Daystart + ChunkSizeMs * (ChunkNumber + 1),

  Chunk = {ChunkNumber, Timestamp, ChunkOffset},
  % ?D({new_chunk, Chunk}),
  State1 = State#dbstate{
    chunk_map = ChunkMap ++ [Chunk],
    next_chunk_time = NextChunkTime},
  {ok, State1}.




current_chunk_offset(Offset, #dbstate{chunk_map_offset = ChunkMapOffset} = _State) ->
  Offset - ChunkMapOffset.

write_chunk_offset(ChunkNumber, ChunkOffset, #dbstate{file = File, chunk_map_offset = ChunkMapOffset} = _State) ->
  ByteOffsetLen = ?OFFSETLEN div 8,
  ok = file:pwrite(File, ChunkMapOffset + ChunkNumber*ByteOffsetLen, <<ChunkOffset:?OFFSETLEN/integer>>).


append_full_row({row,Timestamp,Values}, #dbstate{file = File} = State) ->
  Data = pulsedb_format:encode_full_row(Timestamp,Values),
  {ok, _EOF} = file:position(File, eof),
  ok = file:write(File, Data),
  {ok, State#dbstate{
      last_timestamp = Timestamp,
      depth = length(Values),
      last_row = {row,Timestamp,Values}}
  }.

append_delta_row({row, Timestamp, Values}, #dbstate{file = File, last_row = {row,OldTS,OldValues}} = State) ->
  NewValues = setdepth(Values, length(OldValues)),
  MD = {row,Timestamp,NewValues},

  Data = pulsedb_format:encode_delta_row(Timestamp - OldTS, 
    lists:zipwith(fun(V1,V2) -> V2 - V1 end, OldValues, NewValues)),
  {ok, _EOF} = file:position(File, eof),
  ok = file:write(File, Data),
  {ok, State#dbstate{
      last_timestamp = Timestamp,
      last_row = MD}
  }.


setdepth(_Quotes, 0) ->
  [];
setdepth([], Depth) ->
  [0 || _ <- lists:seq(1, Depth)];
setdepth([Q|Quotes], Depth) ->
  [Q|setdepth(Quotes, Depth - 1)].







