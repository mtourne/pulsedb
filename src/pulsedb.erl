%%% @doc Stock database
%%% Designed for continious writing of stock data
%%% with later fast read and fast seek
-module(pulsedb).
-author({"Danil Zagoskin", 'z@gosk.in'}).
-include("../include/pulsedb.hrl").
-include("log.hrl").
-include("pulsedb.hrl").

-type pulsedb() :: {pulsedb_pid, pid()} | term().

-type price() :: float().
-type volume() :: non_neg_integer().
-type quote() :: {price(), volume()}.
-type timestamp() :: non_neg_integer().
-type stock() :: atom().
-type date() :: string().

-type datetime_ms() :: {calendar:date(), time_ms()}.
-type time_ms() :: {calendar:hour(), calendar:minute(), calendar:second(), millisecond()}.
-type millisecond() :: 0..999.

-type market_data() :: #md{}.
-type trade() :: #trade{}.


-export_type([pulsedb/0, price/0, volume/0, quote/0, timestamp/0, stock/0, date/0]).
-export_type([market_data/0, trade/0]).


%% Application configuration
-export([get_value/1, get_value/2]).

%% Querying available data
-export([stocks/0, stocks/1, dates/1, dates/2, common_dates/1, common_dates/2]).
%% Get information about stock/date file
-export([info/1, info/2, info/3]).

%% Writing DB
-export([open_append/3, append/2, close/1, write_events/3, write_events/4]).
%% Reading existing data
-export([open_read/2, events/1, events/2, events/3, events/4]).
%% Iterator API
-export([init_reader/2, init_reader/3, read_event/1]).

%% Shortcut helpers
-export([timestamp/1, candle/2, candle/3]).

%% Run tests
-export([run_tests/0]).


%% @doc List of stocks in local database
-spec stocks() -> [stock()].
stocks() -> pulsedb_fs:stocks().

%% @doc List of stocks in remote database
-spec stocks(Storage::term()) -> [stock()].
stocks(Storage) -> pulsedb_fs:stocks(Storage).


%% @doc List of available dates for stock
-spec dates(stock()|{any(),stock()}) -> [date()].
dates(Stock) -> pulsedb_fs:dates(Stock).

%% @doc List of available dates in remote database
-spec dates(Storage::term(), Stock::stock()) -> [date()].
dates(Storage, Stock) -> pulsedb_fs:dates(Storage, Stock).

%% @doc List dates when all given stocks have data
-spec common_dates([stock()]) -> [date()].
common_dates(Stocks) -> pulsedb_fs:common_dates(Stocks).

%% @doc List dates when all given stocks have data, remote version
-spec common_dates(Storage::term(), [stock()]) -> [date()].
common_dates(Storage, Stocks) -> pulsedb_fs:common_dates(Storage, Stocks).


%% @doc Open stock for reading
-spec open_read(stock()|{any(),stock()}, date()) -> {ok, pulsedb()} | {error, Reason::term()}.  
open_read(Stock, Date) ->
  pulsedb_reader:open(pulsedb_fs:path(Stock, Date)).

%% @doc Open stock for appending
-spec open_append(stock(), date(), [open_option()]) -> {ok, pulsedb()} | {error, Reason::term()}.  
open_append(Stock, Date, Opts) ->
  Path = pulsedb_fs:path(Stock, Date),
  {db, RealStock, RealDate} = pulsedb_fs:file_info(Path),
  pulsedb_appender:open(Path, [{stock,RealStock},{date,pulsedb_fs:parse_date(RealDate)}|Opts]).

%% @doc Append row to db
-spec append(pulsedb(), trade() | market_data()) -> {ok, pulsedb()} | {error, Reason::term()}.
append(Event, pulsedb) ->
  pulsedb_appender:append(Event, pulsedb).

-spec write_events(stock(), date(), [trade() | market_data()]) -> ok | {error, Reason::term()}.
write_events(Stock, Date, Events) ->
  write_events(Stock, Date, Events, []).

-spec write_events(stock(), date(), [trade() | market_data()], [open_option()]) -> ok | {error, Reason::term()}.
write_events(Stock, Date, Events, Options) ->
  Path = pulsedb_fs:path(Stock, Date),
  {db, RealStock, RealDate} = pulsedb_fs:file_info(Path),
  pulsedb_appender:write_events(Path, Events, [{stock,RealStock},{date,pulsedb_fs:parse_date(RealDate)}|Options]).

%% @doc Fetch information from opened pulsedb
-spec info(pulsedb()) -> [{Key::atom(), Value::term()}].
info(pulsedb) ->
  pulsedb_reader:file_info(pulsedb).

%% @doc Fetch typical information about given Stock/Date
-spec info(stock(), date()) -> [{Key::atom(), Value::term()}].
info(Stock, Date) ->
  pulsedb_reader:file_info(pulsedb_fs:path(Stock, Date)).

%% @doc Fetch requested information about given Stock/Date
-spec info(stock(), date(), [Key::atom()]) -> [{Key::atom(), Value::term()}].
info(Stock, Date, Fields) ->
  pulsedb_reader:file_info(pulsedb_fs:path(Stock, Date), Fields).


%% @doc Get all events from filtered stock/date
-spec events({node, node()}, stock(), date(), [term()]) -> list(trade() | market_data()).
events({node, Node}, Stock, Date, Filters) ->
  {ok, Iterator} = rpc:call(Node, pulsedb, init_reader, [Stock, Date, Filters]),
  events(Iterator).


%% @doc Get all events from filtered stock/date
-spec events(stock(), date(), [term()]) -> list(trade() | market_data()).
events(Stock, Date, Filters) ->
  case init_reader(Stock, Date, Filters) of
    {ok, Iterator} ->
      events(Iterator);
    {error, nofile} ->
      []
  end.

%% @doc Read all events for stock and date
-spec events(stock(), date()) -> list(trade() | market_data()).
events(Stock, Date) ->
  events(Stock, Date, []).

%% @doc Just read all events from pulsedb
-spec events(pulsedb()|iterator()) -> list(trade() | market_data()).
events(#dbstate{} = pulsedb) ->
  {ok, Iterator} = init_reader(pulsedb, []),
  events(Iterator);

events(Iterator) ->
  pulsedb_iterator:all_events(Iterator).

%% @doc Init iterator over opened pulsedb
% Options: 
%    {range, Start, End}
%    {filter, FilterFun, FilterArgs}
% FilterFun is function in pulsedb_filters
-spec init_reader(pulsedb(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(#dbstate{} = pulsedb, Filters) ->
  case pulsedb_iterator:init(pulsedb) of
    {ok, Iterator} ->
      init_reader(Iterator, Filters);
    {error, _} = Error ->
      Error
  end;

init_reader(Iterator, Filters) ->
  {ok, apply_filters(Iterator, Filters)}.

%% @doc Shortcut for opening iterator on stock-date pair
-spec init_reader(stock(), date(), list(reader_option())) -> {ok, iterator()} | {error, Reason::term()}.
init_reader(Stock, Date, Filters) ->
  case open_read(Stock, Date) of
    {ok, pulsedb} ->
      init_reader(pulsedb, Filters);
    {error, _} = Error ->
      Error
  end.


apply_filter(Iterator, false) -> Iterator;
apply_filter(Iterator, {range, Start, End}) ->
  pulsedb_iterator:set_range({Start, End}, Iterator);
apply_filter(Iterator, {filter, Function, Args}) ->
  pulsedb_iterator:filter(Iterator, Function, Args).

apply_filters(Iterator, []) -> Iterator;
apply_filters(Iterator, [Filter|MoreFilters]) ->
  apply_filters(apply_filter(Iterator, Filter), MoreFilters).


%% @doc Read next event from iterator
-spec read_event(iterator()) -> {ok, trade() | market_data(), iterator()} | {eof, iterator()}.
read_event(Iterator) ->
  pulsedb_iterator:read_event(Iterator).

%% @doc close pulsedb
-spec close(pulsedb()) -> ok.
close(#dbstate{file = F} = _pulsedb) ->
  case F of 
    undefined -> ok;
    _ -> file:close(F)
  end,
  ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec candle(stock(), date()) -> {price(),price(),price(),price()}.
candle(Stock, Date) ->
  candle(Stock, Date, []).


-spec candle(stock(), date(), list(reader_option())) -> {price(),price(),price(),price()}.
candle(Stock, Date, Options) ->
  pulsedb_helpers:candle(Stock, Date, Options).

% convert datetime or now() to millisecond timestamp
-spec timestamp(calendar:datetime()|datetime_ms()|erlang:timestamp()) -> timestamp().
timestamp(DateTime) ->
  pulsedb_helpers:timestamp(DateTime).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Configuration
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
%% @doc get configuration value with fallback to given default
get_value(Key, Default) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined -> Default
  end.

%% @private
%% @doc get configuration value, raise error if not found
get_value(Key) ->
  case application:get_env(?MODULE, Key) of
    {ok, Value} -> Value;
    undefined -> erlang:error({no_key,Key})
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%       Testing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @private
run_tests() ->
  eunit:test({application, pulsedb}).

