-module(pulsedb_tempo).
-include("pulsedb.hrl").
-export([open/1, append/2, read/3, close/1]).

-define(TIMEOUT, 10000).

open(Path) ->
  {URL, AuthToken, SeriesId} = parse_url(Path),
  DB = #tempodb{url = URL, 
                auth = AuthToken, 
                series = SeriesId},
  {ok, DB}.


append({_, _, _, _}=Tick, #tempodb{}=DB) ->
  append([Tick], DB);

append(Ticks, #tempodb{}=DB) when is_list(Ticks) ->
  Data = [begin
            SeriesKey = series_name([{column,Name}|lists:usort(Tags)]),
            [{key, <<SeriesKey/binary, ".series">>}, 
             {t, utc_to_iso8601(UTC)}, 
             {v, Value}]
            end || {Name, UTC, Value, Tags} <- Ticks],
  write(DB, Data),
  DB.



read(Name, Query0, #tempodb{url=BaseUrl, auth=AuthToken}=DB) ->
  Query = pulsedb:parse_query(Query0),
  Hdrs = [{"authorization", <<"Basic ", AuthToken/binary>>}],
  URL = iolist_to_binary([BaseUrl, "/data/?", query_attrs([{column,Name}|Query])]),
  {ok, {{200, _}, _, ResponseBody}} = lhttpc:request(URL, get, Hdrs, ?TIMEOUT),
  Data = jsx:decode(ResponseBody),
  io:format("RESPONSE ~p", [Data]),
  {ok, [], DB}.

close(#tempodb{}) ->
  ok.



write(#tempodb{url=BaseUrl, auth=AuthToken}, Data) ->
  URL = iolist_to_binary([BaseUrl, "/multi/"]),
  Hdrs = [{"authorization", <<"Basic ", AuthToken/binary>>}],
  Body = jsx:encode(Data),
  {ok, {{200, _}, _, _}} = lhttpc:request(URL, post, Hdrs, Body, ?TIMEOUT).
  

parse_url(BasePath) ->
  {ok, ParsedUrl} = http_uri:parse(BasePath),
  {Scheme, UserInfo, Host, Port, Path, _Query} = ParsedUrl,
  URL = iolist_to_binary(io_lib:format("~p://~s:~B~s", [Scheme, Host, Port, Path])),
  [Key, Secret, SeriesId] = string:tokens(UserInfo, ":"),
  AuthToken = base64:encode(Key++":"++Secret),
  {URL, AuthToken, SeriesId}.



utc_to_iso8601(UTC) ->
  GS = UTC + calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  {{Y,M,D},{HH,MM,SS}} = calendar:gregorian_seconds_to_datetime(GS),
  iolist_to_binary(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B.000+0000", [Y, M, D, HH, MM, SS])).


series_name([Hd|Rest]) ->
  lists:foldl(fun (Tag, Acc) ->
                Tagname = tagname(Tag),
                <<Acc/binary, ".", Tagname/binary>>
              end, tagname(Hd), Rest).


tagname({Tag, Name}) ->
  iolist_to_binary(io_lib:format("~p:~s", [Tag, Name])).


query_attrs([Attr]) -> 
  format_attr(Attr);

query_attrs([Hd|Rest]) ->
  Attr = format_attr(Hd),
  Attrs = query_attrs(Rest),
  <<Attr/binary, "&", Attrs/binary>>.

  
format_attr({from, UTC0}) -> 
  UTC = utc_to_iso8601(UTC0),
  <<"start=", UTC/binary>>;
  
format_attr({to, UTC0}) -> 
  UTC = utc_to_iso8601(UTC0),
  <<"end=", UTC/binary>>;

format_attr({Attr, Value}) -> 
  iolist_to_binary(io_lib:format("attr[~p]=~s", [Attr, Value])).





