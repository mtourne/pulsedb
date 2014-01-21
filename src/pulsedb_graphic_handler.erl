-module(pulsedb_graphic_handler).
-behaviour(cowboy_websocket_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-export([pulse_data/4, make_queries/1, resolve_embed/2]).

-record(ws_state, {
  last_utc = [],
  pulses
}).

-record(page_state, {
  db,
  resolver,
  embed
}).

init({tcp, http}, Req, Opts) -> 
  {Path, Req2} = cowboy_req:path_info(Req),
  case Path of
    [_,<<"events">>] -> {upgrade, protocol, cowboy_websocket};
    [Embed]        -> {ok, Req2, #page_state{db = proplists:get_value(db,Opts), embed=Embed, resolver=proplists:get_value(resolver, Opts)}};
    _              -> {shutdown, Req2, undefined}
  end.

terminate(_,_,_) ->
  ok.

handle(Req, #page_state{resolver={Resolver,Resolve}, embed=Embed, db = DB}=State) ->
  case erlang:apply(Resolver, Resolve, [Embed, []]) of
    {ok, Title, Queries} ->
      {Path, Req5} = cowboy_req:path(Req),
      WsPath = filename:join([Path,"events"]),

      Template = undefined,
      InitData = page_init_data(Embed, Title, Queries, WsPath, DB),
      {ok, Reply} = cowboy_req:reply(200, headers(html), fill_template(Template, InitData), Req5),
      {ok, Reply, State};
    _ -> 
      {ok, Reply} = cowboy_req:reply(404, headers(html), "not found", Req),
      {ok, Reply, State}
  end.
      

websocket_init(_Transport, Req, _Opts) ->
  {Ip, Req1} = case cowboy_req:header(<<"x-real-ip">>, Req) of
    {undefined, Req_} ->
      {{PeerAddr,_}, Req1_} = cowboy_req:peer(Req_),
      {list_to_binary(inet_parse:ntoa(PeerAddr)), Req1_};
    {PeerAddr, Req1_} ->
      {PeerAddr, Req1_}
  end,
  put(name, {pulsedb_graph,Ip}),
  {ok, Req1, #ws_state{}}.
 

websocket_handle({text, Text}, Req, State) ->
  websocket_handle({'text:json', jsx:decode(Text)}, Req, State);

websocket_handle({'text:json', Json}=Data, Req, #ws_state{}=State) ->
  case proplists:get_value(<<"mfa">>, Json) of
    MFA when is_binary(MFA) -> 
      {Module, Function, Args} = depickle(MFA),
      case erlang:apply(Module, Function, Args) of
        {ok, InitBody, NewState} ->
          %InitBody = graph_init_data(Config),
          {reply, {text, jsx:encode(InitBody)}, Req, NewState};
        _ ->
          {shutdown, Req, State}
      end;
    undefined -> 
      lager:info("Unknown request ~p", [Data]),
      {ok, Req, State}
  end;

websocket_handle(Data, Req, State) -> 
  lager:info("Unknown request ~p", [Data]),
  {ok, Req, State}.



pulse_data(Embed, Title, Queries, DB) ->
  {History, PulseTokens, LastUTCs1} = lists:unzip3(
  [begin
    {Name, QueryRealtime, QueryHistory} = make_queries(Query),
    {ok,History1,_} = pulsedb:read(QueryHistory, DB),
    HistoryData = [[T*1000, V] || {T, V} <- History1],

    Token = make_ref(),
    pulsedb:subscribe(QueryRealtime, Token),
    lager:info("Subscribed websocket to pulse [~p]~s", [Embed, QueryRealtime]),
     
    Link = {Name, Token},
    History2 = [{name,Name},{data, HistoryData}],

    LastUTC = case History1 of
      [] -> undefined;
      _ -> [{Token, element(1,lists:last(History1)) }]
    end,
     
     {History2, Link, LastUTC}
    end
   || Query <- Queries]),
  
  
  LastUTCs = [L || L <- LastUTCs1, is_tuple(L)],
  Config = [
    {title, Title},
    {legend, true},
    {type, spline},
    {navigator, true}
  ],
  Reply = [{init, true}, {options, Config}, {data, History}],
  {ok, Reply, #ws_state{pulses=PulseTokens, last_utc = LastUTCs}}.



websocket_info({pulse, Token, UTC, Value}, Req, #ws_state{pulses=Pulses, last_utc = LastUTCs}=State) ->
  case lists:keyfind(Token, 2, Pulses) of
    false -> 
      {noreply, State};
    {Name,Token} ->
      case lists:keyfind(Token, 1, LastUTCs) of
        {_, LastUTC} when LastUTC - UTC >= 0 ->
          {noreply, State};
        _ ->
          Points = [UTC*1000,Value],
          Prepared = [{shift,true}, {Name, [Points]}],
          {reply, {text, jsx:encode(Prepared)}, Req, State}
      end
  end;


websocket_info(Msg, Req, #ws_state{}=State) ->
  lager:info("Unknown message ~p", [Msg]),
  {ok, Req, State}.



websocket_terminate(_Reason, _Req, _State) -> 
  ok.



page_init_data(Embed, Title, Queries, WsPath, DB) ->
  MFA = {?MODULE,pulse_data,
         [Embed, Title, Queries, DB]},
  [{title, Title},
   {mfa, pickle(MFA)},
   {ws_path, WsPath}].

fill_template(_Template, Data) ->
Template = <<
"<html>
  <head>
    <script src='/js/jquery.js'></script>
    <script src='/js/highcharts.js'></script>
    <script src='/js/graphic.js'></script>
    <script>
      window.onload = function(){ window.Graphic.ws_request('pulse', '{{mfa}}', '{{ws_path}}'); };
    </script>
  </head>
  <body>
    <div id='pulse'></div>
  </body>
</html>">>,
  lists:foldl(fun ({Name_, Value}, T) ->
                Name = iolist_to_binary(io_lib:format("{{~p}}", [Name_])),
                re:replace(T, Name, Value, [{return, binary}])
              end,
              Template, Data).


headers(html) -> [{<<"content-type">>, <<"text/html">>}].

pickle(Term) ->
  Bin = erlang:term_to_binary(Term, [compressed]),
  base64:encode(Bin).

depickle(Encoded) ->
  Bin = base64:decode(Encoded),
  erlang:binary_to_term(Bin).


make_queries(Query0) ->
  {Now,_} = pulsedb:current_second(),
  {_,_,Name,_} = Query1 = pulsedb_query:parse(Query0),
  
  Query2 = pulsedb_query:remove_tag([from, to], Query1),
  QueryRealtime = Query2,
  QueryHistory = pulsedb_query:add_tag({from, Now-360}, QueryRealtime),
  {Name,
   pulsedb_query:render(QueryRealtime),
   pulsedb_query:render(QueryHistory)}.


resolve_embed(Embed, _Opts) ->
  {ok, <<"test embed">>, [Embed]}.
