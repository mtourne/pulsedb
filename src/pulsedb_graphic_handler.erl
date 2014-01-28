-module(pulsedb_graphic_handler).
-behaviour(cowboy_websocket_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-export([pulse_subscribe/4, pulse_history/4, make_queries/2]).

-define(HTTP_REQUEST_TIMEOUT, 5000).
-define(WS_TIMEOUT, 3000).

-define(WS_PATH, <<"events">>).
-define(HTTP_PATH, <<"history">>).

-record(ws_state, {
  last_utc = [],
  pulses,
  db,
  timer,
  ip,
  resolver,
  auth
}).

-record(page_state, {
  embed,
  resolver,
  auth,
  mode,
  db
}).


init_state(#page_state{}=State, Opts) ->
  State#page_state{db = proplists:get_value(db, Opts),
                   auth = lists:keyfind(auth, 1, Opts),
                   resolver = lists:keyfind(resolver, 1, Opts)};

init_state(#ws_state{}=State, Opts) ->
  State#ws_state{db = proplists:get_value(db, Opts),
                 auth = lists:keyfind(auth, 1, Opts),
                 resolver = lists:keyfind(resolver, 1, Opts)}.

init({tcp, http}, Req, Opts) -> 
  {Path, Req2} = cowboy_req:path_info(Req),
  case Path of
    [_,?WS_PATH]       -> {upgrade, protocol, cowboy_websocket};
    [Embed,?HTTP_PATH] -> {ok, Req2, init_state(#page_state{embed = Embed, mode=history}, Opts)};
    [Embed]            -> {ok, Req2, init_state(#page_state{embed = Embed, mode=subscribe}, Opts)};
    _                  -> {shutdown, Req2, undefined}
  end.

terminate(_,_,_) ->
  ok.

%%%%%%
% PAGE
%%%%%%
handle(Req, #page_state{embed=Embed, resolver=Resolver, auth=Auth, mode=subscribe}=State) ->
  {ok, Reply} = 
  case resolve(Embed,Resolver,Auth) of
    {ok, Title, Queries} ->
      {Path, Req5} = cowboy_req:path(Req),
      InitData = page_initial_data(Title, Embed, Path, Queries),
      Page = fill_template(ok, InitData),
      cowboy_req:reply(200, headers(html), Page, Req5);
    {deny, Message} ->
      cowboy_req:reply(403, headers(html), fill_template(denied, [{message, Message}]), Req);
    {not_found, Message} -> 
      cowboy_req:reply(404, headers(html), fill_template(not_found, [{message, Message}]), Req);
    {error, Message} -> 
      lager:warning("embed resolving error ~p", [Message]),
      cowboy_req:reply(500, headers(html), fill_template(error, []), Req)
  end,
  {ok, Reply, State};


%%%%%%%%%
% HISTORY
%%%%%%%%%
handle(Req, #page_state{embed=Embed, resolver=Resolver, auth=Auth, mode=history}=State) ->
  {ok, Reply} = 
  case resolve(Embed,Resolver,Auth) of
    {ok, Title, Queries} ->
      {Step, Req1} = cowboy_req:header(<<"x-use-step">>, Req),
      Opts = use_step(Step),
      case pulse_history(Title, Queries, State, Opts) of
        {ok, InitBody} ->
          cowboy_req:reply(200, headers(json), jsx:encode(InitBody), Req1);
        Other ->
          lager:info("pulse history problem: ~p", [Other]),
          cowboy_req:reply(200, headers(json), "", Req1)
      end;
    {deny, Message} ->
      cowboy_req:reply(403, headers(json), jsx:encode([{error, Message}]), Req);
    {not_found, Message} -> 
      cowboy_req:reply(404, headers(json), jsx:encode([{error, Message}]), Req);
    {error, Message} -> 
      lager:warning("embed resolving error ~p", [Message]),
      cowboy_req:reply(500, headers(json), jsx:encode([{error, Message}]), Req)
  end,
  {ok, Reply, State}.



%%%%%%%%%%%
% WEBSOCKET
%%%%%%%%%%%
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
  {ok, Req1, init_state(#ws_state{ip = Ip}, Opts), 2*?WS_TIMEOUT}.


websocket_handle({pong, _}, Req, #ws_state{} = State) ->
  Ref = erlang:send_after(?WS_TIMEOUT, self(), ping),
  {ok, Req, State#ws_state{timer = Ref}};  


websocket_handle({text, Text}, Req, #ws_state{resolver=Resolver, auth=Auth}=State) ->
  Json = jsx:decode(Text),
  Embed = proplists:get_value(<<"embed">>, Json),
  Opts = use_step(proplists:get_value(<<"use_step">>, Json)),
    
  {ok, Title, Queries} = resolve(Embed, Resolver, Auth),
  case pulse_subscribe(Title, Queries, State, Opts) of
    {ok, InitBody, NewState} ->
      {reply, {text, jsx:encode(InitBody)}, Req, NewState};
    _ ->
      {shutdown, Req, State}
  end;

websocket_handle(Data, Req, State) -> 
  lager:info("Unknown request ~p", [Data]),
  {ok, Req, State}.


%%%%%%%%%%%%%%%%%%%%
% WEBSOCKET MESSAGES
%%%%%%%%%%%%%%%%%%%%
websocket_info({pulse, Token, UTC, Value}, Req, #ws_state{pulses=Pulses, last_utc = LastUTCs}=State) ->
  case lists:keyfind(Token, 2, Pulses) of
    false -> 
      {ok, Req, State};
    {Name,Token} ->
      case lists:keyfind(Token, 1, LastUTCs) of
        {_, LastUTC} when LastUTC - UTC >= 0 ->
          {ok, Req, State};
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



%%%%%%%%%
% BACKEND
%%%%%%%%%
pulse_subscribe(Title, Queries, #ws_state{db = DB} = State, Opts) ->
  {History, PulseTokens, LastUTCs1} = lists:unzip3(
  [begin
    {Name, QueryRealtime, QueryHistory} = make_queries(Query, Opts),
     
     
    {ok,History1,_} = pulsedb:read(QueryHistory, DB),
    HistoryData = [[T*1000, V] || {T, V} <- History1],

    Token = make_ref(),
    pulsedb:subscribe(QueryRealtime, Token),
    lager:info("Subscribed websocket ~p to pulse ~s", [get(name), QueryRealtime]),
     
    Link = {Name, Token},
    History2 = [{name,Name},{data, HistoryData}],

    LastUTC = case History1 of
      [] -> undefined;
      _ -> {Token, element(1,lists:last(History1))}
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


pulse_history(Title, Queries, #page_state{db=DB}, Opts) ->
  History = [begin
     {Name, _, QueryHistory} = make_queries(Query, Opts),
     {ok,History1,_} = pulsedb:read(QueryHistory, DB),
     HistoryData = [[T*1000, V] || {T, V} <- History1],
     [{name,Name},{data, HistoryData}]
  end || Query <- Queries],

  Config = [{title, Title}],
  Reply = [{init, true}, {options, Config}, {data, History}],
  {ok, Reply}.




fill_template(Status, Data) ->
  TemplateName = iolist_to_binary(io_lib:format("templates/embed_~p.html", [Status])),
  Path = filename:join(code:lib_dir(pulsedb,webroot), TemplateName),
  {ok, Template} = file:read_file(Path),
  lists:foldl(fun ({Name_, Value}, T) ->
                Name = iolist_to_binary(io_lib:format("{{~p}}", [Name_])),
                re:replace(T, Name, Value, [{return, binary}])
              end,
              Template, Data).


headers(json) -> [{<<"content-type">>,<<"application/json">>}];
headers(html) -> [{<<"content-type">>, <<"text/html">>}].


page_initial_data(Title, Embed, Path, Queries) ->
  Step = lists:foldl(fun (Q, Max) ->
                       Q1 = pulsedb_query:parse(Q),
                       max(Max, pulsedb_query:downsampler_step(Q1))
                     end, 1, Queries),
  Protocol = if 
    Step > 5 -> <<"http">>;
    true     -> <<"ws">> 
  end,
  [{title, Title},
   {protocol, Protocol}, 
   {step, jsx:encode(null)},
   {config, jsx:encode([{embed, Embed},
                        {container, <<"pulse">>},
                        {range_selector, <<"range">>},
                        {ws_path, filename:join([Path, ?WS_PATH])}, 
                        {http_path, filename:join([Path, ?HTTP_PATH])}])}].


use_step(Value) when is_number(Value) -> 
  [{step, Value}];

use_step(Value) when is_binary(Value) ->
  try [{step, binary_to_integer(Value)}]
  catch _:_ -> []
  end;

use_step(_) -> [].



make_queries(Query0, Opts) ->
  {Now,_} = pulsedb:current_second(),
  Q1 = pulsedb_query:parse(Query0),
  Step = proplists:get_value(step, Opts, pulsedb_query:downsampler_step(Q1)),
  To   = Now - 4,
  From = To - Step * 60,
  
  Q2 = 
  if 
    Step > 1 -> pulsedb_query:set_step(Step, Q1);
    true -> Q1
  end,
  
  QueryRealtime = pulsedb_query:remove_tag([from, to], Q2),
  QueryHistory = pulsedb_query:set_range(From, To, Q2),
  

  
  Name = pulsedb_query:remove_tag([<<"account">>], QueryRealtime),
  
  {pulsedb_query:render(Name),
   pulsedb_query:render(QueryRealtime),
   pulsedb_query:render(QueryHistory)}.


%%%%%%%%%%%%%%%%%%%%%%%%%%
% RESOLVING AND DECRYPTING
%%%%%%%%%%%%%%%%%%%%%%%%%%

resolve(Embed, Resolver, Auth) ->
  case pulsedb_embed_cache:read(Embed) of
    {ok, Value} -> 
      Value;
    _ -> 
      Result = try resolve_embed(Embed, Resolver) of
        {ok, Resolved} ->
          try decrypt_embed(Resolved, Auth)
          catch C:E -> {error, {C,E}}
          end;
        Other -> 
          Other
      catch
        C:E -> {error, {C,E}}
      end,
      pulsedb_embed_cache:write(Embed, Result),
      Result
  end.





resolve_embed(<<"full-", Embed/binary>>, _) ->
  {ok, Embed};

resolve_embed(ShortEmbed, {resolver, url, BaseURL}) ->
  URL = iolist_to_binary([BaseURL, "/", ShortEmbed]),
  case lhttpc:request(URL, get, [], ?HTTP_REQUEST_TIMEOUT) of
    {ok,{{200,_},_,Embed}}  -> {ok, Embed};
    {ok,{{403,_},_,Reason}} -> {deny, Reason};
    {ok,{{404,_},_,Reason}} -> {not_found, Reason};
    {error, Reason}         -> {error, Reason};
    Other                   -> {error, Other}
  end;

resolve_embed(ShortEmbed, {resolver, Module, ResolveFn}) ->
  Module:ResolveFn(ShortEmbed);

resolve_embed(Embed, false) ->
  {ok, Embed}.


decrypt_embed(Embed, {auth,AuthModule,AuthArgs}) ->
  {ok, Json} = AuthModule:decrypt(Embed, AuthArgs),
  Data = jsx:decode(Json),
  {ok, proplists:get_value(<<"title">>, Data),
       proplists:get_value(<<"queries">>, Data, [])};

decrypt_embed(Embed, false) ->
  {ok, <<"Graphic">>, [Embed]}.