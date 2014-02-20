-module(pulsedb_disk).
-author('Max Lapshin <max@maxidoors.ru>').

-include("pulsedb.hrl").


-record(source, {
  id :: non_neg_integer(),
  name :: source_name(),
  original_name :: source_name(),
  original_tags :: list(),
  offsets_offset :: non_neg_integer(),
  block_offsets = [] :: non_neg_integer()
}).

-type source() :: #source{}.



-record(disk_db, {
  storage = pulsedb_disk,
  path :: file:filename(),
  date :: binary() | undefined,
  sources :: [source()],
  cached_source_names = [],

  mode :: undefined | read | append,

  config_fd :: file:fd(),
  data_fd :: file:fd()
}).



-export([open/2, append/2, read/3, close/1, sync/1]).
-export([info/1]).
-export([delete_older/2, hours/3]).
-export([shift_value/1, unshift_value/1]).
-export([metric_name/2, metric_fits_query/2, aggregate/2, downsample/2]).


-spec open(Path::file:filename(), Options::list()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path, Options) when is_list(Path) ->
  open(iolist_to_binary(Path), Options);
open(<<"file://", Path/binary>>, Options) ->
  open(Path, Options);
open(Path, _Options) when is_binary(Path) ->
  {ok, #disk_db{path = Path}}.


open0(#disk_db{path = Path, config_fd = undefined, date = Date} = DB, Mode) when Date =/= undefined ->
  case filelib:is_regular(filename:join([Path,Date,config_v4])) of
    true ->
      try open_existing_db(DB#disk_db{mode = Mode})
      catch
        Class:Error ->
          Trace = erlang:get_stacktrace(),
          lager:log(error,[{module,?MODULE},{line,?LINE}],"Error in pulsedb: ~p:~p, need to recover for date: ~p\n~p\n", [Class, Error, Date, Trace]),
          file:delete(filename:join([Path,Date,config_v4])),
          file:delete(filename:join([Path,Date,data_v4])),
          erlang:raise(Class, Error, Trace)
      end;
    false when Mode == read ->
      DB#disk_db{mode = read};
    false when Mode == append ->
      create_new_db(DB#disk_db{mode = append})
  end.


create_new_db(#disk_db{path = Path, date = Date, mode = append} = DB) when Date =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v4]),
  DataPath = filename:join([Path, Date, data_v4]),

  case filelib:ensure_dir(ConfigPath) of
    ok -> ok;
    {error, Reason1} -> throw({error, {create_path_failed,ConfigPath,Reason1}})
  end,

  Opts = [binary,write,exclusive,raw],

  {ok, ConfigFd} = case file:open(ConfigPath, Opts) of
    {ok, CFile_} -> {ok, CFile_};
    {error, Reason2} -> throw({error,{open_config_failed,ConfigPath,Reason2}})
  end,

  {ok, DataFd} = file:open(DataPath, Opts ++ [{delayed_write, 128000, 5000}]),

  DB#disk_db{path = Path, config_fd = ConfigFd, data_fd = DataFd, sources = []}.


read_file(Path) ->
  case file:read_file(Path) of
    {ok, Bin} -> Bin;
    {error, _} -> <<>>
  end.

open_existing_db(#disk_db{path = Path, date = Date, mode = Mode} = DB) when Date =/= undefined, Mode =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v4]),
  DataPath = filename:join([Path, Date, data_v4]),

  DB1 = case DB#disk_db.sources of
    undefined ->
      DB#disk_db{sources = decode_config(read_file(ConfigPath))};
    _ ->
      DB
  end,

  Opts = case Mode of
    append -> [binary,read,write,raw];
    read -> [binary,read,raw]
  end,

  {ok, ConfigFd} = file:open(ConfigPath, Opts),
  {ok, DataFd} = file:open(DataPath, Opts),

  DB1#disk_db{config_fd = ConfigFd, data_fd = DataFd}.




sync(#disk_db{config_fd = ConfigFd, data_fd = DataFd} = DB) ->
  case ConfigFd of
    undefined -> ok;
    _ -> file:sync(ConfigFd)
  end,
  case DataFd of
    undefined -> ok;
    _ -> file:sync(DataFd)
  end,
  {ok, DB}.


-spec append(pulsedb:tick() | [pulsedb:tick()], pulsedb:db()) -> pulsedb:db().

append([{_,_,_,_} = Tick|Ticks], #disk_db{} = DB) ->
  {ok, DB1} = append(Tick, DB),
  append(Ticks, DB1);

append([], #disk_db{} = DB) ->
  {ok, DB};

append(_, #disk_db{mode = read, path = Path}) ->
  error({need_to_reopen_pulsedb_for_append,Path});

append({Name, UTC, Value, _Tags} = T, #disk_db{}) when not is_binary(Name); not is_integer(UTC); not is_integer(Value) ->
  error({invalid_tick, T});

append({_Name, UTC, _Value, _Tags} = Tick, #disk_db{config_fd = undefined, date = undefined} = DB) ->
  append(Tick, open0(DB#disk_db{date = pulsedb_time:date_path(UTC)}, append));

append({_, UTC, _, _} = Tick, #disk_db{mode = append, date = Date, path = Path} = DB) ->
  UTCDate = pulsedb_time:date_path(UTC),
  {ok, DB1} = if
    Date == undefined orelse UTCDate == Date ->
      try append0(Tick, DB)
      catch
        Class:Error ->
          Trace = erlang:get_stacktrace(),
          lager:log(error,[{module,?MODULE},{line,?LINE}],"Error in pulsedb: ~p:~p, need to recover for date: ~s\n~p\n", [Class, Error, UTCDate, Trace]),
          file:delete(filename:join([Path,UTCDate,config_v4])),
          file:delete(filename:join([Path,UTCDate,data_v4])),
          erlang:raise(Class, Error, Trace)
      end;
    true ->
      {ok, DB_} = close(DB),
      append(Tick, DB_)
  end,
  {ok, DB1}.



append0({Name, UTC, Value, Tags}, #disk_db{} = DB) ->
  {ok, SourceId, DB1} = find_or_open_source(Name, Tags, DB),
  {ok, DB2} = append_data(SourceId, UTC, Value, DB1),
  {ok, DB2}.


find_or_open_source(Name, Tags, #disk_db{sources = S} = DB) ->
  {SourceName, DB1} = source_name(Name, Tags, DB),
  case lists:keyfind(SourceName, #source.name, S) of
    false -> append_new_source(SourceName, Name, Tags, DB1);
    #source{id = SourceId} -> {ok, SourceId, DB1}
  end.


source_name(Name, Tags, #disk_db{cached_source_names = SourceNames} = DB) ->
  case lists:keyfind({Name,Tags}, 1, SourceNames) of
    {_, SourceName} ->
      {SourceName, DB};
    false ->
      SourceName = metric_name(Name, Tags),
      CachedSourceNames = if 
        length(SourceNames) > 100 -> [{{Name,Tags},SourceName}] ++ lists:sublist(SourceNames, 90);
        true -> [{{Name,Tags},SourceName}] ++ SourceNames
      end,
      {SourceName, DB#disk_db{cached_source_names = CachedSourceNames}}
  end.



append_new_source(SourceName, Name, Tags, #disk_db{sources = Sources, config_fd = ConfigFd} = DB) ->
  {ok, ConfigPos} = file:position(ConfigFd, eof),
  Source = #source{id = length(Sources), name = SourceName, offsets_offset = ConfigPos + 5 + size(SourceName),
    original_name = Name, original_tags = Tags},
  Bin = encode_config(Source),
  ok = file:pwrite(ConfigFd, ConfigPos, Bin),
  {ok, Source#source.id, DB#disk_db{sources = Sources ++ [Source]}}.


-define(CONFIG_SOURCE, 4).

encode_config(#source{name = Name, block_offsets = Offsets}) ->
  L = size(Name),
  OffsetsBin = pack_offsets(Offsets),
  Conf = [<<L:16, Name:L/binary>>, OffsetsBin],
  Size = iolist_size(Conf),
  iolist_to_binary([<<?CONFIG_SOURCE, Size:16>>, Conf]).


pack_offsets(Offsets) ->
  [<<(proplists:get_value(I, Offsets, -1)):32/signed>> || I <- lists:seq(0,23)].

-spec decode_config(binary()) -> [source()].

decode_config(Bin) when is_binary(Bin) ->
  decode_config(Bin, 0, 0).

decode_config(<<?CONFIG_SOURCE, Length:16, Source:Length/binary, Rest/binary>>, ConfigOffset, Id) ->
  <<L:16, Name:L/binary, OffsetsBin:96/binary>> = Source,
  Offsets =[ {I,O} || {I,O} <- lists:zip(lists:seq(0,23), [O || <<O:32/signed>> <= OffsetsBin]), O >= 0],

  [OriginalName|Tags] = binary:split(Name, <<":">>, [global]),
  OriginalTags = lists:map(fun(T) ->
    [K,V] = binary:split(T, <<"=">>),
    {K,V}
  end, Tags),
  [#source{id = Id, name = Name, block_offsets = Offsets, offsets_offset = ConfigOffset + 1 + 2 + 2 + L,
    original_name = OriginalName, original_tags = OriginalTags}
    |decode_config(Rest, ConfigOffset + 1 + 2 + Length, Id + 1)];

decode_config(<<>>, _, _) ->
  [].



metric_name(Name, Tags) ->
 iolist_to_binary([to_b(Name), [[":",to_b(K),"=",to_b(V)] ||  {K,V} <- lists:sort(Tags)]]).

to_b(Atom) when is_atom(Atom) -> atom_to_binary(Atom, latin1);
to_b(Bin) when is_binary(Bin) -> Bin.

append_data(SourceId, UTC, Value, #disk_db{data_fd = DataFd} = DB) ->
  #disk_db{sources = Sources} = DB1 = open_hour_if_required(SourceId, UTC, DB),
  #source{block_offsets = Offsets} = lists:keyfind(SourceId,#source.id,Sources),
  Hour = (UTC rem 86400) div 3600,

  {_, BlockOffset} = lists:keyfind(Hour, 1, Offsets),

  Second = UTC rem 3600,

  ok = file:pwrite(DataFd, (BlockOffset bsl 13) + Second*2, shift_value(Value)),
  {ok, DB1}.


% length(integer_to_list((16#1000000-1) bsr 10,2)) should be between 1 and 14

shift_value(Value) when Value >= 0              andalso Value < 16#4000 -> <<3:2, Value:14>>;
shift_value(Value) when Value >= 16#1000        andalso Value < 16#1000000 -> <<2:2, (Value bsr 10):14>>;
shift_value(Value) when Value >= 16#100000      andalso Value < 16#400000000 -> <<1:2, (Value bsr 20):14>>;
shift_value(Value) when Value >= 16#10000000    andalso Value < 16#80000000000 -> <<0:2, 1:1, (Value bsr 30):13>>;
shift_value(Value) when Value >= 16#10000000000 andalso Value < 16#20000000000000 -> <<0:2, 0:1, (Value bsr 40):13>>.

unshift_value(Bin) when is_binary(Bin) ->
  [{_,V}] = unpack_ticks(Bin, 0),
  V.



open_hour_if_required(SourceId, UTC, #disk_db{config_fd = ConfigFd, data_fd = DataFd, sources = Sources} = DB) ->
  Hour = (UTC rem 86400) div 3600,
  #source{block_offsets = Offsets, name = Name, offsets_offset = O} = Source = lists:keyfind(SourceId,#source.id,Sources),
  case proplists:get_value(Hour, Offsets) of
    Offset when is_number(Offset) andalso Offset >= 0 ->
      DB;
    _ ->
      {ok, DataPos} = file:position(DataFd, eof),
      BlockOffset = case DataPos rem 8192 of
        0 ->
          DataPos;
        _ ->
          lager:error("Error with data file for ~p / ~p. Last offset is ~p", [DB#disk_db.path, DB#disk_db.date, DataPos]),
          ((DataPos div 8192) + 1)*8192
      end,
      AdditionalInfo = case iolist_to_binary([pulsedb_time:date_path(UTC)," ", integer_to_list(Hour), " ", Name]) of
        <<Info:991/binary, _/binary>> -> <<Info/binary, 0>>;
        Info -> [Info, binary:copy(<<0>>, 992 - size(Info))]
      end,
      ok = file:pwrite(DataFd, BlockOffset + 7200, AdditionalInfo),
      Offsets1 = lists:keystore(Hour, 1, Offsets, {Hour, BlockOffset bsr 13}),
      ok = file:pwrite(ConfigFd, O, pack_offsets(Offsets1)),

      Sources1 = lists:keystore(SourceId, #source.id, Sources, Source#source{block_offsets = Offsets1}),
      DB#disk_db{sources = Sources1}
  end.






info(#disk_db{sources = Sources}) when is_list(Sources) ->
  Src = lists:sort([{Name,lists:sort(Tags)} || #source{original_name = Name, original_tags = Tags} <- Sources]),
  [{sources,Src}];

info(#disk_db{path = Path} = DB) ->
  case last_day(Path) of
    undefined -> [];
    DayPath -> info(DB#disk_db{sources = decode_config(read_file(filename:join(DayPath,config_v4)))})
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





-spec read(Name::source_name(), Query::[{binary(),binary()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.

% read(_Name, _Query, #disk_db{mode = append}) ->
%   {error, need_to_reopen_for_read};

read(Name, Query, #disk_db{path = Path, date = Date} = DB) ->
  RequiredDates = required_dates(Query),
  case load_ticks(RequiredDates, Name, Query, #disk_db{path = Path, date = Date}) of
    {ok, Ticks, DB1} ->
      close(DB1),
      {ok, Ticks, DB};
    {error, _} = Error ->
      Error
  end.



required_dates(Query) ->
  {from,From} = lists:keyfind(from,1,Query),
  {to,To} = lists:keyfind(to,1,Query),
  [pulsedb_time:date_path(X*86400) || X <- lists:seq(From div 86400,To div 86400)].


load_ticks([], _Name, _Query, DB) ->
  {ok, [], DB};

load_ticks([Date|Dates], Name, Query, #disk_db{} = DB) ->
  case read0(Name, Query, DB#disk_db{date = Date}) of
    {ok, Ticks1, DB1} ->
      {ok, DB2} = close(DB1),
      case load_ticks(Dates, Name, Query, DB2) of
        {ok, Ticks2, DB3} ->
          {ok, Ticks1++Ticks2, DB3};
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      Error
  end.





read0(Name, Query, #disk_db{config_fd = undefined, date = Date} = DB) when Date =/= undefined ->
  case open0(DB, read) of
    #disk_db{config_fd = undefined} = DB1 ->
      {ok, [], DB1};
    #disk_db{} = DB1 ->
      read0(Name, Query, DB1)
  end;

read0(Name, Query, #disk_db{sources = Sources, data_fd = DataFd, date = Date, mode = read} = DB) when Date =/= undefined ->
  Tags = [{K,V} || {K,V} <- Query, is_binary(K)],

  ReadSources = select_sources(Name, Tags, Sources),

  DateUTC = pulsedb_time:parse(Date),
  From = proplists:get_value(from,Query, pulsedb_time:daystart(DateUTC)),
  To = proplists:get_value(to,Query, pulsedb_time:daystart(DateUTC)+86400 - 1),

  HrsToRead = hours(From, To, DateUTC),


  Ticks1 = lists:flatmap(fun({H,BlockOffset,Limit}) ->
    Ticks2 = lists:flatmap(fun(#source{block_offsets = Offsets}) ->
      case lists:keyfind(H, 1, Offsets) of
        {H, Offset} ->
          case file:pread(DataFd, Offset bsl 13 + 2*BlockOffset, 2*Limit) of
            {ok, Bin} ->
              unpack_ticks(Bin, DateUTC + H*3600 + BlockOffset);
            _ ->
              []
          end;
        false ->
          []
      end        
    end, ReadSources),

    Ticks3 = lists:sort(Ticks2),

    Ticks4 = aggregate(proplists:get_value(aggregator,Query), Ticks3),
    erlang:garbage_collect(self()),
    Ticks4
  end, HrsToRead),
  
  Ticks5 = downsample(proplists:get_value(downsampler,Query), Ticks1),
  {ok, Ticks5, DB}.


hours(From, To, Date) ->
  F = From div 3600,
  T = To div 3600,
  DateStartD = Date div 86400,
  
  [begin
     Offset = if 
       F == H -> (From - H*3600) rem 3600;
       true -> 0 end,
     
     Length = if
       Offset > 0     -> 3600 - Offset;
       T == F, T == H -> To - From + 1;
       T == H         -> To - H*3600 + 1;
       true           -> 3600 end,
     
     {H rem 24, Offset, Length}
     end || H <- lists:seq(F, T), H div 24 == DateStartD].



unpack_ticks(<<>>, _) -> [];
unpack_ticks(<<0:16, Rest/binary>>, UTC) -> unpack_ticks(Rest, UTC+1);
unpack_ticks(<<3:2, Value:14, Rest/binary>>, UTC) -> [{UTC,Value}|unpack_ticks(Rest, UTC+1)];
unpack_ticks(<<2:2, Value:14, Rest/binary>>, UTC) -> [{UTC,Value bsl 10}|unpack_ticks(Rest, UTC+1)];
unpack_ticks(<<1:2, Value:14, Rest/binary>>, UTC) -> [{UTC,Value bsl 20}|unpack_ticks(Rest, UTC+1)];
unpack_ticks(<<0:2, 1:1, Value:13, Rest/binary>>, UTC) -> [{UTC,Value bsl 30}|unpack_ticks(Rest, UTC+1)];
unpack_ticks(<<0:2, 0:1, Value:13, Rest/binary>>, UTC) -> [{UTC,Value bsl 40}|unpack_ticks(Rest, UTC+1)].




metric_fits_query(Query, Tags) ->
  [] == [K || {K,V} <- Query, proplists:get_value(K,Tags) =/= V].



select_sources(_Name, _Tags, []) ->
  [];
select_sources(Name, Tags, [#source{original_name = Name, original_tags = Tags1} = S|Sources]) ->
  case metric_fits_query(Tags, Tags1) of
    true -> [S|select_sources(Name, Tags, Sources)];
    _ -> select_sources(Name, Tags, Sources)
  end;

select_sources(Name, Tags, [_|Sources]) ->
  select_sources(Name, Tags, Sources).


aggregate(Aggegator, Ticks) ->
  Agg = case Aggegator of
    undefined -> fun sum/1;
    <<"sum">> -> fun sum/1;
    <<"avg">> -> fun avg/1;
    <<"max">> -> fun max/1;
    Else -> error({unknown_aggregator,Else})
  end,
  aggregate(Agg, Ticks, undefined, []).


aggregate(_Agg, [], undefined, []) -> [];
aggregate(Agg, [], UTC, Acc) -> [{UTC,Agg(Acc)}];
aggregate(Agg, [{UTC,V1}|Ticks], undefined, []) -> aggregate(Agg, Ticks, UTC, [V1]);
aggregate(Agg, [{UTC,V1}|Ticks], UTC, Acc) -> aggregate(Agg, Ticks, UTC, [V1|Acc]);
aggregate(Agg, [{UTC2,V1}|Ticks], UTC1, Acc) ->
  [{UTC1,Agg(Acc)}|aggregate(Agg, Ticks, UTC2, [V1])].


sum([]) -> 0;
sum(Acc) -> lists:sum(Acc).

avg([]) -> 0;
avg(Acc) -> lists:sum(Acc) div length(Acc).

max([]) -> 0;
max(Acc) -> lists:max(Acc).




downsample(undefined, Ticks) ->
  Ticks;

downsample({Step, Downsampler}, Ticks) ->
  D = case Downsampler of
    undefined -> fun sum/1;
    <<"sum">> -> fun sum/1;
    <<"avg">> -> fun avg/1;
    <<"max">> -> fun max/1;
    Else -> error({unknown_aggregator,Else})
  end,
  downsample(D, Ticks, Step, undefined, []).


downsample(_D, [], _Step, undefined, []) -> [];
downsample(D, [], _Step, UTC, Acc) -> [{UTC,D(Acc)}];
downsample(D, [{UTC,V}|Ticks], Step, undefined, []) -> downsample(D, Ticks, Step, (UTC div Step)*Step, [V]);
downsample(D, [{UTC,V}|Ticks], Step, N, Acc) when (UTC div Step)*Step == N -> downsample(D, Ticks, Step, N, [V|Acc]);
downsample(D, [{UTC,V}|Ticks], Step, N, Acc) -> [{N,D(Acc)}|downsample(D, Ticks, Step, (UTC div Step)*Step, [V])].




-spec close(pulsedb:db()) -> {pulsedb:db()}.
close(#disk_db{config_fd = C, data_fd = D} = DB) ->
  file:close(C),
  file:close(D),
  {ok, DB#disk_db{config_fd = undefined, data_fd = undefined,
    date = undefined, sources = undefined}}.




delete_older(Time, #disk_db{path = Path} = DB) ->
  {Now, _} = pulsedb:current_second(),
  GoodDate = binary_to_list(pulsedb_time:date_path(Now - Time)),
  Dates = [Date || Date <- filelib:wildcard("*/*/*", binary_to_list(Path)), Date < GoodDate],

  [begin
    file:delete(filename:join([Path,Date,config_v4])),
    file:delete(filename:join([Path,Date,data_v4])),
    file:del_dir(filename:join(Path,Date)),
    file:del_dir(filename:join(Path,filename:dirname(Date))),
    file:del_dir(filename:join(Path,filename:dirname(filename:dirname(Date)))),
    ok
  end || Date <- Dates],

  {ok, DB}.





