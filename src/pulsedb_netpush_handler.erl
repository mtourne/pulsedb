-module(pulsedb_netpush_handler).
-author('Max Lapshin <max@maxidoors.ru>').

% comment it to compile without cowboy
%-behaviour(cowboy_http_handler).


-export([init/3, upgrade/4, terminate/3]).
-export([handle_info/2, terminate/2]).



init({_,http}, Req, _) ->
  lager:info("Connected server"),
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
  socket
}).


upgrade(Req, _Env, _Mod, Args) ->
  % TODO: authentication

  Req5 = Req,
  {Ip, Req6} = case cowboy_req:header(<<"x-real-ip">>, Req5) of
    {undefined, Req5_} ->
      {{PeerAddr,_}, Req6_} = cowboy_req:peer(Req5_),
      {list_to_binary(inet_parse:ntoa(PeerAddr)), Req6_};
    {PeerAddr, Req6_} ->
      {PeerAddr, Req6_}
  end,


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

  State = #netpush{ip = Ip, transport = Transport, socket = Socket, db = DB},
  gen_server:enter_loop(?MODULE, [], State).


handle_info({ssl,Socket,Bin}, #netpush{socket = Socket} = State) when is_binary(Bin) ->
  Len = size(Bin) - 1,
  <<Bin1:Len/binary, "\n">> = Bin,
  State1 = #netpush{} = case handle_msg(Bin1, State) of
    #netpush{} = S -> S;
    {reply, Msg, #netpush{} = S} -> ssl:send(Socket, [Msg, "\n"]), S
  end,
  ssl:setopts(Socket, [{active,once}]),
  {noreply, State1};


handle_info({tcp,Socket,Bin}, #netpush{socket = Socket} = State) when is_binary(Bin) ->
  Len = size(Bin) - 1,
  <<Bin1:Len/binary, "\n">> = Bin,
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

handle_msg(<<"utc ", UTC_/binary>>, #netpush{} = State) ->
  UTC = erlang:binary_to_integer(UTC_),
  State#netpush{utc = UTC};

handle_msg(<<"metric ", Msg/binary>>, #netpush{metrics = Metrics} = State) ->
  [MetricId, Metric] = binary:split(Msg, <<" ">>),
  [Name|Tags_] = binary:split(Metric, <<":">>, [global]),
  Tags = [ list_to_tuple(binary:split(T,<<"=">>)) || T <- Tags_],
  Metrics1 = lists:keystore(MetricId, 1, Metrics, {MetricId, {Name,Tags}}),
  State#netpush{metrics = Metrics1};

handle_msg(<<C,_/binary>> = Msg, #netpush{metrics = Metrics, utc = UTC0, db = DB} = State) when C >= $0 andalso C =< $9 ->
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
  {ok, DB1} = pulsedb:append({Name,UTC,Value,Tags}, DB),
  State#netpush{utc = UTC, db = DB1};


handle_msg(Bin, #netpush{} = State) ->
  lager:info("msg: ~p\n", [Bin]),
  State.








