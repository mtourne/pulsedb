%%% @doc Pulse database
%%% Designed for continious writing of pulse data
%%% with later fast read and fast seek
-module(pulsedb).
-author({"Danil Zagoskin", 'z@gosk.in'}).
-author("Max Lapshin <max@maxidoors.ru>").
-include("../include/pulsedb.hrl").
-include("log.hrl").
-include("pulsedb.hrl").

-type pulsedb() :: {pulsedb_pid, pid()} | term().

-type value() :: integer().
-type timestamp() :: non_neg_integer().
-type date() :: string().

-type datetime_ms() :: {calendar:date(), time_ms()}.
-type time_ms() :: {calendar:hour(), calendar:minute(), calendar:second(), millisecond()}.
-type millisecond() :: 0..999.


-type row() :: {row, Timestamp::timestamp(), [value()]}.

-export_type([pulsedb/0, value/0, timestamp/0, date/0, row/0]).


%% Writing DB
-export([open_append/1, append/2, close/1]).

% -export([write_events/2]).

%% Reading existing data
-export([events/2, event_columns/2, info/1, info/2]).
% open_read/1, 


% %% Iterator API
% -export([init_reader/2, init_reader/3, read_event/1]).



% %% @doc Open stock for reading
% -spec open_read(stock()|{any(),stock()}, date()) -> {ok, pulsedb()} | {error, Reason::term()}.  
% open_read(Path) ->
%   pulsedb_reader:open(Path).



%% @doc Open stock for appending
-spec open_append(file:path()) -> {ok, pulsedb()} | {error, Reason::term()}.  
open_append(Path) ->
  pulsedb_sliding_appender:open(Path).

%% @doc Append row to db
-spec append(row(), pulsedb()) -> {ok, pulsedb()} | {error, Reason::term()}.
append(Event, Pulsedb) ->
  pulsedb_sliding_appender:append(Event, Pulsedb).

% -spec write_events(path(), [row()]) -> ok | {error, Reason::term()}.
% write_events(Path, Events) ->
%   write_events(Path, Events, []).

% -spec write_events(path(), [row()], [open_option()]) -> ok | {error, Reason::term()}.
% write_events(Path, Events, Options) ->
%   pulsedb_appender:write_events(Path, Events, Options).

% %% @doc Fetch information from opened pulsedb
% -spec info(pulsedb()) -> [{Key::atom(), Value::term()}].
% info(Pulsedb) ->
%   pulsedb_reader:file_info(Pulsedb).


info(Path) ->
  pulsedb_reader:file_info(Path).

info(Path, Options) ->
  pulsedb_reader:file_info(Path, Options).


%% @doc Get all events as rows
-spec events(file:path(), date()) -> list(row()).
events(Path, Date) ->
  case pulsedb_iterator:open(Path, [{date,Date}]) of
    {ok, Iterator} -> events(Iterator);
    {error,Reason} -> []
  end.


%% @doc Get all events as columns for graphic library
-spec event_columns(file:path(), date()) -> [{Name::binary(), [{timestamp(),value()}] }].
event_columns(Path, Date) ->
  {{Y,M,D},_} = pulsedb_time:date_time(Date),
  RealPath = lists:flatten(io_lib:format("~s/~4..0B/~2..0B/~2..0B.pulse", [Path, Y,M,D])),
  transpose(RealPath, events(Path, Date)).

transpose(_Path, []) -> [];
transpose(Path, [{row,_,Values}|_] = Rows) ->
  Info = pulsedb_reader:file_info(Path),
  Indexes = lists:seq(1,length(Values)),
  Columns = proplists:get_value(columns, Info, Indexes),
  [ {lists:nth(I,Columns), [{TS,lists:nth(I,Vals)} || {row,TS,Vals} <- Rows] } || I <- Indexes].




% %% @doc Just read all events from pulsedb
% -spec events(pulsedb()|iterator()) -> list(row()).
% events(#dbstate{} = Stockdb) ->
%   {ok, Iterator} = init_reader(Stockdb, []),
%   events(Iterator);

events(Iterator) ->
  pulsedb_iterator:all_events(Iterator).

%% @doc Init iterator over opened pulsedb
% Options: 
%    {range, Start, End}
%    {filter, FilterFun, FilterArgs}
% FilterFun is function in pulsedb_filters
% -spec init_reader(file:path(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
% init_reader(Path, Options) ->
%   (Path, Options).

% init_reader(Iterator, Filters) ->
%   {ok, apply_filters(Iterator, Filters)}.

% %% @doc Shortcut for opening iterator on stock-date pair
% -spec init_reader(stock(), date(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
% init_reader(Stock, Date, Filters) ->
%   case open_read(Stock, Date) of
%     {ok, Pulsedb} ->
%       init_reader(Pulsedb, Filters);
%     {error, _} = Error ->
%       Error
%   end.



% %% @doc Read next event from iterator
% -spec read_event(iterator()) -> {ok, row(), iterator()} | {eof, iterator()}.
% read_event(Iterator) ->
%   pulsedb_iterator:read_event(Iterator).

%% @doc close pulsedb
-spec close(pulsedb()) -> ok.
close(#dbstate{file = undefined}) -> ok;
close(#dbstate{file = F}) ->
  file:close(F).



