-module(pulsedb_netpush_handler).
-author('Max Lapshin <max@maxidoors.ru>').

% comment it to compile without cowboy
%-behaviour(cowboy_http_handler).


-export([init/3, upgrade/4, terminate/3]).
-export([handle_info/2, terminate/2]).


init(_, Req, [status]) ->
  {ok, Req2} = cowboy_req:reply(200, [], <<"Running\n">>, Req),
  {shutdown, Req2, undefined};



init({_,http}, Req, _Args) ->
  {Upgrade, Req1} = cowboy_req:header(<<"upgrade">>, Req),
  case Upgrade of
    <<"application/timeseries-text">> ->
      {upgrade, protocol, ?MODULE};
    _Else ->
      lager:debug("Invalid upgrade request: ~p", [_Else]),
      {ok, Req2} = cowboy_req:reply(409, [], <<"Bad request">>, Req1),
      {shutdown, Req2, undefined}
  end.


terminate(_,_,_) -> ok.
terminate(_,_) -> ok.


-record(netpush, {
  ip,
  transport,
  db,
  utc,
  metrics = [],
  user_tags = [],
  socket,
  tracker_id
}).



upgrade(Req, Env, Mod, Args) ->
  try upgrade0(Req, Env, Mod, Args)
  catch
    exit:normal ->
      {halt, Req};
    C:E ->
      lager:error("error in server login: ~p:~p\n~p", [C,E, erlang:get_stacktrace()]),
      {error, 500, Req}
  end.



upgrade0(Req, Env, _Mod, Args) ->
  {_, ListenerPid} = lists:keyfind(listener, 1, Env),
  case erlang:function_exported(ranch_listener, remove_connection, 1) of
    true -> ranch_listener:remove_connection(ListenerPid);
    false -> ranch:remove_connection(ListenerPid)
  end,

  % TODO: authentication

  {Headers, Req1} = cowboy_req:headers(Req),

  UserTags = case lists:keyfind(auth, 1, Args) of
    {auth,Module,AuthArgs} ->
      case Module:auth(Headers, AuthArgs) of
        {ok, Yes} -> Yes;
        {error, denied} ->
          {ok, _} = cowboy_req:reply(403, [], "Denied\n", Req1),
          exit(normal)
      end;
    false ->
      []
  end,

  Req5 = Req1,

  {Ip, Req6} = case cowboy_req:header(<<"x-real-ip">>, Req5) of
    {undefined, Req5_} ->
      {{PeerAddr,_}, Req6_} = cowboy_req:peer(Req5_),
      {list_to_binary(inet_parse:ntoa(PeerAddr)), Req6_};
    {PeerAddr, Req6_} ->
      {PeerAddr, Req6_}
  end,


  TrackerId = case proplists:get_value(<<"point">>, UserTags) of
    undefined -> {unknown, make_ref()};
    Value -> Value
  end,

  gen_tracker:add_existing_child(pulsedb_pushers, {TrackerId, self(), worker, []}),
  gen_tracker:setattr(pulsedb_pushers, TrackerId,
                      [{account,proplists:get_value(<<"account">>, UserTags)},
                       {ip, Ip},
                       {user_tags, UserTags}]),

  {ok, Req8} = cowboy_req:upgrade_reply(101, [{<<"upgrade">>,<<"application/timeseries-text">>}], Req6),
  receive
    {cowboy_req,resp_sent} -> ok
  after
    1000 -> error(timeout_open)
  end,

  [Socket,Transport] = cowboy_req:get([socket,transport], Req8),
  Transport:setopts(Socket, [{active,once},{packet,line}]),

  put('$ancestors', [self()]),
  put(name, {pulsedb_netpush, Ip}),

  {ok, DB} = case proplists:get_value(db, Args) of
    undefined ->
      case proplists:get_value(path,Args) of
        undefined -> {ok, undefined};
        DBPath -> pulsedb:open(DBPath)
      end;
    DB_ ->
      {ok, DB_}
  end,

  lager:info("Accepted connection from ~s with info: ~p", [Ip, UserTags]),
  State = #netpush{ip = Ip, transport = Transport, socket = Socket,
                   db = DB, user_tags = UserTags, tracker_id = TrackerId},
  gen_server:enter_loop(?MODULE, [], State).


handle_info({ssl,Socket,Bin}, #netpush{socket = Socket, tracker_id = TrackerId} = State) when is_binary(Bin) ->
  Len = size(Bin) - 1,
  <<Bin1:Len/binary, "\n">> = Bin,

  {UTCLocal,_} = pulsedb:current_second(),
  gen_tracker:setattr(pulsedb_pushers, TrackerId, [{last_contact_at,UTCLocal}]),

  State1 = #netpush{} = case handle_msg(Bin1, State) of
    #netpush{} = S -> S;
    {reply, Msg, #netpush{} = S} -> ssl:send(Socket, [Msg, "\n"]), S
  end,
  ssl:setopts(Socket, [{active,once}]),
  {noreply, State1};


handle_info({tcp,Socket,Bin}, #netpush{socket = Socket, tracker_id = TrackerId} = State) when is_binary(Bin) ->
  Len = size(Bin) - 1,
  <<Bin1:Len/binary, "\n">> = Bin,

  {UTCLocal,_} = pulsedb:current_second(),
  gen_tracker:setattr(pulsedb_pushers, TrackerId, [{last_contact_at,UTCLocal}]),

  State1 = #netpush{} = case handle_msg(Bin1, State) of
    #netpush{} = S -> S;
    {reply, Msg, #netpush{} = S} -> gen_tcp:send(Socket, [Msg, "\n"]), S
  end,
  inet:setopts(Socket, [{active,once}]),
  {noreply, State1};

handle_info({tcp_closed, _Socket}, #netpush{} = State) ->
  {stop, normal, State};

handle_info({ssl_closed, _Socket}, #netpush{} = State) ->
  {stop, normal, State};

handle_info(Msg, #netpush{} = State) ->
  lager:info("unknown msg ~p", [Msg]),
  {noreply, State}.



handle_msg(<<"ping ", N/binary>>, #netpush{} = State) ->
  {reply, <<"pong ", N/binary>>, State};

handle_msg(<<"utc ", UTC_/binary>>, #netpush{tracker_id=TrackerId} = State) ->
  UTC = erlang:binary_to_integer(UTC_),
  {UTCLocal,_} = pulsedb:current_second(),
  UTCDelta = UTCLocal - UTC,
  gen_tracker:setattr(pulsedb_pushers, TrackerId, [{utc_delta,UTCDelta}]),
  State#netpush{utc = UTC};

handle_msg(<<"metric ", Msg/binary>>, #netpush{metrics = Metrics} = State) ->
  [MetricId, Metric] = binary:split(Msg, <<" ">>),
  [Name|Tags_] = binary:split(Metric, <<":">>, [global]),
  Tags = [ list_to_tuple(binary:split(T,<<"=">>)) || T <- Tags_],
  Metrics1 = lists:keystore(MetricId, 1, Metrics, {MetricId, {Name,Tags}}),
  State#netpush{metrics = Metrics1};

handle_msg(<<C,_/binary>> = Msg, #netpush{metrics = Metrics, utc = UTC0, db = DB, user_tags = UserTags} = State) when C >= $0 andalso C =< $9 ->
  [MetricId, UTCDelta, Val] = binary:split(Msg, <<" ">>, [global]),
  {_,{Name, Tags}} = lists:keyfind(MetricId, 1, Metrics),
  UTC = UTC0 + binary_to_integer(UTCDelta),
  L = size(Val) - 1,
  Value = case Val of
    <<V:L/binary, "K">> -> binary_to_integer(V) bsl 10;
    <<V:L/binary, "k">> -> binary_to_integer(V) bsl 10;
    <<V:L/binary, "M">> -> binary_to_integer(V) bsl 20;
    <<V:L/binary, "m">> -> binary_to_integer(V) bsl 20;
    <<V:L/binary, "G">> -> binary_to_integer(V) bsl 30;
    <<V:L/binary, "g">> -> binary_to_integer(V) bsl 30;
    <<V:L/binary, "T">> -> binary_to_integer(V) bsl 40;
    <<V:L/binary, "t">> -> binary_to_integer(V) bsl 40;
    _ -> binary_to_integer(Val)
  end,
  Tags1 = UserTags ++ [{T,V} || {T,V} <- Tags, lists:keyfind(T,1,UserTags) == false],
  pulsedb:append({Name,UTC,Value,Tags1}, seconds),
  case pulsedb:append({Name,UTC,Value,Tags1}, DB) of
    {ok, DB1} -> State#netpush{utc = UTC, db = DB1};
    undefined -> State#netpush{utc = UTC}
  end;


handle_msg(Bin, #netpush{} = State) ->
  lager:info("msg: ~p\n", [Bin]),
  State.








