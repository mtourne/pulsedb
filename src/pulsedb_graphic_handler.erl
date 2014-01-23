-module(pulsedb_graphic_handler).
-behaviour(cowboy_websocket_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-export([pulse_data/3, make_queries/1, resolve_embed/3]).

-define(HTTP_REQUEST_TIMEOUT, 5000).
-define(WS_TIMEOUT, 3000).

-record(ws_state, {
  last_utc = [],
  pulses,
  db,
  timer,
  ip,
  auth,
  embed_resolver
}).

-record(page_state, {
  embed,
  auth,
  embed_resolver
}).

fill_state(#page_state{}=State, Opts) ->
  State#page_state{auth = lists:keyfind(auth, 1, Opts),
                   embed_resolver = lists:keyfind(embed_resolver, 1, Opts)};

fill_state(#ws_state{}=State, Opts) ->
  State#ws_state{db = proplists:get_value(db,Opts), 
                 auth = lists:keyfind(auth, 1, Opts),
                 embed_resolver = lists:keyfind(embed_resolver, 1, Opts)}.


init({tcp, http}, Req, Opts) -> 
  {Path, Req2} = cowboy_req:path_info(Req),
  case Path of
    [_,<<"events">>] -> {upgrade, protocol, cowboy_websocket};
    [Embed]          -> {ok, Req2, fill_state(#page_state{embed = Embed}, Opts)};
    _                -> {shutdown, Req2, undefined}
  end.

terminate(_,_,_) ->
  ok.

handle(Req, #page_state{embed=Embed, auth=Auth, embed_resolver=URL}=State) ->
  {ok, Reply} = 
  case resolve_embed(Embed, Auth, [{embed_resolver, URL}]) of
    {ok, Title, _} ->
      {Path, Req5} = cowboy_req:path(Req),
      InitData = [{title, Title},
                  {embed, Embed},
                  {ws_path, filename:join([Path,"events"])}],
      Page = fill_template(page, InitData),
      cowboy_req:reply(200, headers(html), Page, Req5);
    {deny, Reason} ->
      cowboy_req:reply(403, headers(html), Reason, Req);
    {not_found, Reason} -> 
      cowboy_req:reply(404, headers(html), Reason, Req);
    {error, Reason} -> 
      cowboy_req:reply(404, headers(html), Reason, Req)
  end,
  {ok, Reply, State}.


websocket_init(_Transport, Req, Opts) ->
  {Ip, Req1} = case cowboy_req:header(<<"x-real-ip">>, Req) of
    {undefined, Req_} ->
      {{PeerAddr,_}, Req1_} = cowboy_req:peer(Req_),
      {list_to_binary(inet_parse:ntoa(PeerAddr)), Req1_};
    {PeerAddr, Req1_} ->
      {PeerAddr, Req1_}
  end,
  put(name, {pulsedb_graph,Ip}),
  self() ! init,
  {ok, Req1, fill_state(#ws_state{ip = Ip}, Opts), 2*?WS_TIMEOUT}.


websocket_handle({pong, _}, Req, #ws_state{} = State) ->
  Ref = erlang:send_after(?WS_TIMEOUT, self(), ping),
  {ok, Req, State#ws_state{timer = Ref}};  


websocket_handle({text, Text}, Req, #ws_state{auth=Auth, embed_resolver=URL}=State) ->
  Json = jsx:decode(Text),
  Embed = proplists:get_value(<<"embed">>, Json),
  {ok, Title, Queries} = resolve_embed(Embed, Auth, [{embed_resolver, URL}]),
  case pulse_data(Title, Queries, State) of
    {ok, InitBody, NewState} ->
      {reply, {text, jsx:encode(InitBody)}, Req, NewState};
    _ ->
      {shutdown, Req, State}
  end;

websocket_handle(Data, Req, State) -> 
  lager:info("Unknown request ~p", [Data]),
  {ok, Req, State}.



pulse_data(Title, Queries, #ws_state{db = DB} = State) ->
  {History, PulseTokens, LastUTCs1} = lists:unzip3(
  [begin
    {Name, QueryRealtime, QueryHistory} = make_queries(Query),
    {ok,History1,_} = pulsedb:read(QueryHistory, DB),
    HistoryData = [[T*1000, V] || {T, V} <- History1],

    Token = make_ref(),
    pulsedb:subscribe(QueryRealtime, Token),
    lager:info("Subscribed websocket ~p to pulse ~s", [get(name), QueryRealtime]),
     
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
    {title, Title}
  ],
  Reply = [{init, true}, {options, Config}, {data, History}],
  {ok, Reply, State#ws_state{pulses=PulseTokens, last_utc = LastUTCs}}.



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

websocket_info(init, Req, #ws_state{} = State) ->
  Ref = erlang:send_after(?WS_TIMEOUT, self(), ping),
  {ok, Req, State#ws_state{timer = Ref}};

websocket_info(ping, Req, #ws_state{timer = Old} = State) ->
  erlang:cancel_timer(Old),
  {reply, {ping, <<>>}, Req, State#ws_state{timer = undefined}};


websocket_info(Msg, Req, #ws_state{}=State) ->
  lager:info("Unknown message ~p", [Msg]),
  {ok, Req, State}.



websocket_terminate(Reason, _Req, #ws_state{ip = Ip}) ->
  lager:info("Closing graphic connection to ip ~s due to ~p", [Ip, Reason]),
  ok.



fill_template(page, Data) ->
Template = <<
"<html>
  <head>
    <script src='/js/jquery.js'></script>
    <script src='/js/highcharts.js'></script>
    <script src='/js/graphic.js'></script>
    <script>
      window.onload = function(){ window.Graphic.ws_request('pulse', '{{embed}}', '{{ws_path}}'); };
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


headers(json) -> [{<<"content-type">>,<<"application/json">>}];
headers(html) -> [{<<"content-type">>, <<"text/html">>}].



make_queries(Query0) ->
  {Now,_} = pulsedb:current_second(),
  {_,_,Name,_} = Query1 = pulsedb_query:parse(Query0),
  
  Query2 = pulsedb_query:remove_tag([from, to], Query1),
  QueryRealtime = Query2,
  QueryHistory1 = pulsedb_query:add_tag({from, Now-360}, QueryRealtime),
  QueryHistory2 = pulsedb_query:add_tag({to, Now-4}, QueryHistory1),
  {Name,
   pulsedb_query:render(QueryRealtime),
   pulsedb_query:render(QueryHistory2)}.


resolve_embed(<<"full-", Embed/binary>>, Auth, Opts) ->
  decrypt_embed(Embed, Auth, Opts);

resolve_embed(ShortEmbed, Auth, Opts) ->
  case proplists:get_value(embed_resolver, Opts) of
    {embed_resolver, BaseURL} ->
      URL = iolist_to_binary([BaseURL, "/", ShortEmbed]),
      case lhttpc:request(URL, get, [], ?HTTP_REQUEST_TIMEOUT) of
        {ok,{{200,_},_,Embed}}  -> decrypt_embed(Embed, Auth, Opts);
        {ok,{{404,_},_,Reason}} -> {not_found, Reason};
        {error, Reason}         -> {error, Reason}
      end;
    Other -> 
      lager:warning("unconfigured resolver: ~p", [Other]),
      {error, "Unconfigured resolver"}
  end.


decrypt_embed(Embed, {auth,AuthModule,AuthArgs}, Opts) ->
  try AuthModule:decrypt(Embed, AuthArgs) of
    {ok, Data} -> 
      Json = jsx:decode(Data),
      case is_allowed(Embed, Json, Opts) of
        true -> 
          Title = proplists:get_value(<<"title">>, Json, <<>>),
          Queries = proplists:get_value(<<"queries">>, Json, []),
          {ok, Title, Queries};
        false ->
          {deny, "Not allowed"}
      end;
    Other ->
      lager:warning("can't decrypt embed ~p: ~p", [Embed, Other]),
      {error, "Can't decrypt embed"}
  catch
    C:E -> 
      lager:warning("Can't decrypt embed ~p: ~p", [Embed, {C,E}]),
      {error, "Can't decrypt embed"}
  end;

decrypt_embed(Embed, false, _Opts) ->
  {ok, <<"Graphic">>, [Embed]}.


is_allowed(_Embed, _Json, _Opts) ->
  % #FIXME: check domain, user etc.
  true.