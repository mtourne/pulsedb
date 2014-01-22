-module(pulsedb_graphic_handler).
-behaviour(cowboy_websocket_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-export([pulse_data/3, make_queries/1, resolve_embed/3]).

-record(ws_state, {
  last_utc = [],
  pulses,
  db,
  timer,
  auth
}).

-record(page_state, {
  embed,
  auth
}).

init({tcp, http}, Req, Opts) -> 
  {Path, Req2} = cowboy_req:path_info(Req),
  case Path of
    [_,<<"events">>] -> {upgrade, protocol, cowboy_websocket};
    [Embed]        -> {ok, Req2, #page_state{embed = Embed, 
                                             auth = lists:keyfind(auth, 1, Opts)}};
    _              -> {shutdown, Req2, undefined}
  end.

terminate(_,_,_) ->
  ok.

handle(Req, #page_state{embed=Embed, auth=Auth}=State) ->
  case resolve_embed(Embed, Auth, []) of
    {ok, Title, _} ->
      {Path, Req5} = cowboy_req:path(Req),
      InitData = [{title, Title},
                  {embed, Embed},
                  {ws_path, filename:join([Path,"events"])}],
      Page = fill_template(page, InitData),
      {ok, Reply} = cowboy_req:reply(200, headers(html), Page, Req5),
      {ok, Reply, State};
    {deny, Reason} ->
      {ok, Reply} = cowboy_req:reply(403, headers(html), Reason, Req),
      {ok, Reply, State};
    _ -> 
      {ok, Reply} = cowboy_req:reply(404, headers(html), "not found", Req),
      {ok, Reply, State}
  end.


-define(WS_TIMEOUT, 3000).


websocket_init(_Transport, Req, Opts) ->
  {Ip, Req1} = case cowboy_req:header(<<"x-real-ip">>, Req) of
    {undefined, Req_} ->
      {{PeerAddr,_}, Req1_} = cowboy_req:peer(Req_),
      {list_to_binary(inet_parse:ntoa(PeerAddr)), Req1_};
    {PeerAddr, Req1_} ->
      {PeerAddr, Req1_}
  end,
  put(name, {pulsedb_graph,Ip}),
  DB = proplists:get_value(db,Opts),
  Auth = lists:keyfind(auth, 1, Opts),
  self() ! init,
  {ok, Req1, #ws_state{db = DB, auth = Auth}, 2*?WS_TIMEOUT}.

websocket_handle({pong, _}, Req, #ws_state{} = State) ->
  Ref = erlang:send_after(?WS_TIMEOUT, self(), ping),
  {ok, Req, State#ws_state{timer = Ref}};  

websocket_handle({text, Text}, Req, #ws_state{db=DB, auth=Auth}=State) ->
  Json = jsx:decode(Text),
  Embed = proplists:get_value(<<"embed">>, Json),
  {ok, Title, Queries} = resolve_embed(Embed, Auth, []),
  case pulse_data(Title, Queries, DB) of
    {ok, InitBody, NewState} ->
      {reply, {text, jsx:encode(InitBody)}, Req, NewState};
    _ ->
      {shutdown, Req, State}
  end;

websocket_handle(Data, Req, State) -> 
  lager:info("Unknown request ~p", [Data]),
  {ok, Req, State}.



pulse_data(Title, Queries, DB) ->
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

websocket_info(init, Req, #ws_state{} = State) ->
  Ref = erlang:send_after(?WS_TIMEOUT, self(), ping),
  {ok, Req, State#ws_state{timer = Ref}};

websocket_info(ping, Req, #ws_state{timer = Old} = State) ->
  erlang:cancel_timer(Old),
  {reply, {ping, <<>>}, Req, State#ws_state{timer = undefined}};


websocket_info(Msg, Req, #ws_state{}=State) ->
  lager:info("Unknown message ~p", [Msg]),
  {ok, Req, State}.



websocket_terminate(_Reason, _Req, _State) -> 
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


headers(html) -> [{<<"content-type">>, <<"text/html">>}].



make_queries(Query0) ->
  {Now,_} = pulsedb:current_second(),
  {_,_,Name,_} = Query1 = pulsedb_query:parse(Query0),
  
  Query2 = pulsedb_query:remove_tag([from, to], Query1),
  QueryRealtime = Query2,
  QueryHistory = pulsedb_query:add_tag({from, Now-360}, QueryRealtime),
  {Name,
   pulsedb_query:render(QueryRealtime),
   pulsedb_query:render(QueryHistory)}.


resolve_embed(<<"full-", Embed/binary>>, Auth, Opts) ->
  decrypt_embed(Embed, Auth, Opts);

resolve_embed(ShortEmbed, Auth, Opts) ->
  Embed = ShortEmbed,
  decrypt_embed(Embed, Auth, Opts).


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
    _ ->
      {deny, <<>>}
  catch
    _:_ -> 
      {deny, <<>>}
  end;

decrypt_embed(_, _, _) ->
  {deny, "Auth module unconfigured"}.


is_allowed(_Embed, _Json, _Opts) ->
  % #FIXME: check domain, user etc.
  true.