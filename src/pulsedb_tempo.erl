-module(pulsedb_tempo).
-include("pulsedb.hrl").


-record(tempodb, {
  storage = pulsedb_tempo,
  url,
  auth
}).



-export([open/1, append/2, read/3, close/1]).

-define(TIMEOUT, 10000).

open(Path) ->
  {URL, AuthToken} = parse_url(Path),
  DB = #tempodb{url = URL, auth = AuthToken},
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
  append_data(DB, Data).


read(Name, Query, #tempodb{}=DB) ->
  read_data(Name, Query, DB).


read_data(Name, Query0, #tempodb{url=BaseUrl, auth=AuthToken}=DB) ->
  Query = pulsedb:parse_query(Query0),
  Hdrs = [{"authorization", <<"Basic ", AuthToken/binary>>}],
  URL = iolist_to_binary([BaseUrl, "/data/?", query_attrs([{column,Name}|Query])]),
  Result = case lhttpc:request(URL, get, Hdrs, ?TIMEOUT) of
    {ok, {{200, _}, _, ResponseBody}} ->
      Data = jsx:decode(ResponseBody),
      io:format("RESP ~p~n~p~n", [URL, Data]),
      extract_data(Data);
    {ok, {{StatusCode, ReasonPhrase},_,ResponseBody}} ->
      lager:warning("tempo_db read failed: ~p|~p:~p", [StatusCode, ReasonPhrase, ResponseBody]),
      [];
    {error, Reason} ->
      lager:warning("tempo_db read failed: ~p", [Reason]),
      []
  end,
  {ok, Result, DB}.

close(#tempodb{}) ->
  ok.



append_data(#tempodb{url=BaseUrl, auth=AuthToken}=DB, Data) ->
  URL = iolist_to_binary([BaseUrl, "/multi/"]),
  Hdrs = [{"authorization", <<"Basic ", AuthToken/binary>>}],
  Body = jsx:encode(Data),
  case lhttpc:request(URL, post, Hdrs, Body, ?TIMEOUT) of
    {ok, {{200, _}, _, _}} ->
      ok;
    {ok, {{StatusCode, ReasonPhrase},_,ResponseBody}} ->
      lager:warning("tempo_db write failed: ~p|~p:~p", [StatusCode, ReasonPhrase, ResponseBody]);
    {error, Reason} ->
      lager:warning("tempo_db write failed: ~p", [Reason])
  end,
  {ok, DB}.

parse_url(BasePath) ->
  {ok, ParsedUrl} = http_uri:parse(BasePath),
  {Scheme, UserInfo, Host, Port, Path, _Query} = ParsedUrl,
  URL = iolist_to_binary(io_lib:format("~p://~s:~B~s", [Scheme, Host, Port, Path])),
  AuthToken = base64:encode(UserInfo),
  {URL, AuthToken}.



utc_to_iso8601(UTC) ->
  GS = UTC + calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
  {{Y,M,D},{HH,MM,SS}} = calendar:gregorian_seconds_to_datetime(GS),
  iolist_to_binary(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT"
                                 "~2.10.0B:~2.10.0B:~2.10.0B.000+0000", [Y, M, D, HH, MM, SS])).

iso8601_to_utc(<<YY:4/bytes,"-",MM:2/bytes,"-",DD:2/bytes,"T",
                  H:2/bytes,":", M:2/bytes,":", S:2/bytes,
                    _/binary>>) ->
  DateTime = {{binary_to_integer(YY), binary_to_integer(MM), binary_to_integer(DD)},
              {binary_to_integer(H),  binary_to_integer(M),  binary_to_integer(S)}},
  Seconds = calendar:datetime_to_gregorian_seconds(DateTime),
  Seconds - calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}).



extract_data(Data) ->
  
  Extracted = lists:flatmap(fun (SeriesData) -> series_data(SeriesData) end, Data),
  Sorted = lists:sort(Extracted),
  Sorted.
  

series_data(SeriesData) ->
  Data = proplists:get_value(<<"data">>, SeriesData),
  [{iso8601_to_utc(proplists:get_value(<<"t">>, It)),
                   proplists:get_value(<<"v">>, It)} || It <- Data].





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

format_attr({function, FN}) -> 
  <<"function=", FN/binary>>;

format_attr({interval, I}) -> 
  <<"interval=", I/binary>>;

format_attr({Attr, Value}) -> 
  iolist_to_binary(io_lib:format("attr[~p]=~s", [Attr, escape_special(Value)])).


escape_special(Value) when is_binary(Value) orelse is_list(Value) ->
  join(re:split(Value,"[\.:]"), "_");

escape_special(Value) -> 
  Value.


join([], _) -> <<>>;
join([Value], _) -> Value;
join([Value|Rest_], Separator) ->
  Rest = join(Rest_, Separator),
  <<Value/binary, Separator/binary, Rest/binary>>.


%{ok, DB} = pulsedb_tempo:open("https://c06f0aba43b1449e9f3ae3a5018f572d:ae7457eb892d4d0cbae99a567a42f9f8@api.tempo-db.com/v1").


% pulsedb_tempo:append({<<"input">>,  1389602565, 1,   [{name, "m1"}, {account, "emailA"}, {point, "pnt1"}]}, DB).
% pulsedb_tempo:append({<<"output">>, 1389602565, 2,   [{name, "m1"}, {account, "emailA"}, {point, "pnt1"}]}, DB).
% pulsedb_tempo:append({<<"input">>,  1389602565, 10,  [{name, "m1"}, {account, "emailA"}, {point, "pnt2"}]}, DB).
% pulsedb_tempo:append({<<"output">>, 1389602565, 20,  [{name, "m1"}, {account, "emailA"}, {point, "pnt3"}]}, DB).
% pulsedb_tempo:append({<<"input">>,  1389602565, 100, [{name, "m1"}, {account, "emailA"}, {point, "pnt3"}]}, DB).
% pulsedb_tempo:append({<<"output">>, 1389602565, 200, [{name, "m1"}, {account, "emailA"}, {point, "pnt3"}]}, DB).
% pulsedb_tempo:append({<<"input">>,  1389602566, 5,   [{name, "m1"}, {account, "emailA"}, {point, "pnt1"}]}, DB).
% pulsedb_tempo:append({<<"output">>, 1389602566, 6,   [{name, "m1"}, {account, "emailA"}, {point, "pnt1"}]}, DB).
% pulsedb_tempo:append({<<"input">>,  1389602566, 15,  [{name, "m1"}, {account, "emailA"}, {point, "pnt2"}]}, DB).
% pulsedb_tempo:append({<<"output">>, 1389602566, 26,  [{name, "m1"}, {account, "emailA"}, {point, "pnt3"}]}, DB).
% pulsedb_tempo:append({<<"input">>,  1389602566, 105, [{name, "m1"}, {account, "emailA"}, {point, "pnt3"}]}, DB).
% pulsedb_tempo:append({<<"output">>, 1389602566, 206, [{name, "m1"}, {account, "emailA"}, {point, "pnt3"}]}, DB).

% pulsedb_tempo:read(<<"input">>, [{from, 1389602550},{to, 1389682869}, {account, <<"emailA">>}], DB).


% pulsedb_tempo:read(<<"output">>, [{from, 1389600000},{to, 1389689869}, 
%                                   {account, <<"emailA">>}], DB).

% pulsedb_tempo:read(<<"output">>, [{from, 1389600000},{to, 1389689869}, 
%                                   {account, <<"emailA">>}, 
%                                   {function, <<"sum">>}, {interval, <<"6day">>}], DB).