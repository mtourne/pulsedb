-module(pulsedb_graphic_handler).
-behaviour(cowboy_websocket_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-export([pulse_data/4, make_queries/2, resolve_embed/2]).

-record(ws_state, {
  pulses
  }).

-record(page_state, {
  resolver,
  embed}).

init({tcp, http}, Req, Opts) -> 
  {Path, Req2} = cowboy_req:path_info(Req),
  case Path of
    [_,<<"events">>] -> {upgrade, protocol, cowboy_websocket};
    [Embed]        -> {ok, Req2, #page_state{embed=Embed, resolver=proplists:get_value(resolver, Opts)}};
    _              -> {shutdown, Req2, undefined}
  end.

terminate(_,_,_) ->
  ok.

handle(Req, #page_state{resolver={Resolver,Resolve}, embed=Embed}=State) ->
  case erlang:apply(Resolver, Resolve, [Embed, []]) of
    {ok, Title, Account, Queries} ->
      {Path, Req5} = cowboy_req:path(Req),
      WsPath = filename:join([Path,"events"]),

      Template = undefined,
      InitData = page_init_data(Embed, Title, Account, Queries, WsPath),
      {ok, Reply} = cowboy_req:reply(200, headers(html), fill_template(Template, InitData), Req5),
      {ok, Reply, State};
    _ -> 
      {ok, Reply} = cowboy_req:reply(404, headers(html), "not found", Req),
      {ok, Reply, State}
  end.
      

websocket_init(_Transport, Req, _Opts) -> 
  {ok, Req, #ws_state{}}.
 

websocket_handle({text, Text}, Req, State) ->
  Arg = try {'text:json', jsx:decode(Text)}
  catch _:_ -> {'text:other', Text} 
  end,
  websocket_handle(Arg, Req, State);

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



pulse_data(Embed, Title, Account, Queries) ->
  {History, PulseTokens} = lists:unzip(
  [begin
     {Name, QueryRealtime, QueryHistory} = make_queries(Query,Account),
     {ok,History1,_} = pulsedb:read(QueryHistory, simple_db),
     HistoryData = [[T*1000, V] || {T, V} <- History1],

     Token = make_ref(),
     pulsedb:subscribe(QueryRealtime, Token),
     lager:info("Subscribed websocket to pulse [~p]~s", [Embed, QueryRealtime]),
     
     Link = {Name, Token},
     History2 = [{name,Name},{data, HistoryData}],
     
     {History2, Link}
     end
   || Query <- Queries]),
         
  
  Config = [
    {title, Title},
    {legend, true},
    {type, spline},
    {navigator, true}
  ],
  Reply = [{init, true}, {options, Config}, {data, History}],
  {ok, Reply, #ws_state{pulses=PulseTokens}}.



websocket_info({pulse, Token, UTC, Value}, Req, #ws_state{pulses=Pulses}=State) ->
  case lists:keyfind(Token, 2, Pulses) of
    false -> 
      {noreply, State};
    {Name,Token} ->
      Points = [UTC*1000,Value],
      Prepared = [{shift,true}, {Name, [Points]}],
      {reply, {text, jsx:encode(Prepared)}, Req, State}
  end;


websocket_info(Msg, Req, #ws_state{}=State) ->
  lager:info("Unknown message ~p", [Msg]),
  {ok, Req, State}.



websocket_terminate(_Reason, _Req, _State) -> 
  ok.



page_init_data(Embed, Title, Account, Queries, WsPath) ->
  MFA = {?MODULE,pulse_data,
         [Embed, Title, Account, Queries]},
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


make_queries(Query0, Account) ->
  {Now,_} = pulsedb:current_second(),
  {_,_,Name,_} = Query1 = pulsedb_query:parse(Query0),
  
  Query2 = pulsedb_query:remove_tag([from, to, <<"account">>], Query1),
  QueryRealtime = pulsedb_query:add_tag({account, Account}, Query2),
  QueryHistory = pulsedb_query:add_tag({from, Now-360}, QueryRealtime),
  {Name,
   pulsedb_query:render(QueryRealtime),
   pulsedb_query:render(QueryHistory)}.


resolve_embed(Embed, _Opts) ->
  Query = pulsedb_query:parse(Embed),
  Account = pulsedb_query:tag(<<"account">>, Query),
  {ok, <<"test embed">>, Account, [Embed]}.
