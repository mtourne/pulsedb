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

% -export([open_append/3, append/2, close/1, write_events/2]).
% %% Reading existing data
% -export([open_read/1, events/3]).
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


% %% @doc Get all events from filtered stock/date
% -spec events(path(), date(), date()) -> list(row()).
% events(Stock, From, To) ->
%   {ok, Iterator} = init_reader(Stock, From, To),
%   events(Iterator).

% %% @doc Just read all events from pulsedb
% -spec events(pulsedb()|iterator()) -> list(row()).
% events(#dbstate{} = Stockdb) ->
%   {ok, Iterator} = init_reader(Stockdb, []),
%   events(Iterator);

% events(Iterator) ->
%   pulsedb_iterator:all_events(Iterator).

% %% @doc Init iterator over opened pulsedb
% % Options: 
% %    {range, Start, End}
% %    {filter, FilterFun, FilterArgs}
% % FilterFun is function in pulsedb_filters
% -spec init_reader(pulsedb(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
% init_reader(#dbstate{} = Pulsedb, Filters) ->
%   case pulsedb_iterator:init(Pulsedb) of
%     {ok, Iterator} ->
%       init_reader(Iterator, Filters);
%     {error, _} = Error ->
%       Error
%   end;

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



