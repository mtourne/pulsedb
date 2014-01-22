-module(pulsedb_launcher).

-export([start/0]).
-export([pushers/0]).


workers() ->
  [begin
    {_,Dict} = process_info(Pid,dictionary),
    {Type, IP} = case proplists:get_value(name,Dict) of
      {pulsedb_netpush,I} -> {pusher,I};
      {pulsedb_graph,I} -> {graphic,I};
      _ -> {undefined,undefined}
    end,
    {Pid,Type,IP} 
  end || {_,Pid,_,_} <-  supervisor:which_children(ranch_server:get_connections_sup(fake_pulsedb))].


pushers() ->
  [{Pid,Ip} || {Pid,pusher,Ip} <- workers()].

start() ->
  application:load(pulsedb),

  case file:consult("priv/pulsedb.config") of
    {ok, Env} ->
      [application:set_env(pulsedb, K, V) || {K,V} <- Env];
    _ ->
      ok
  end,


  ok = application:start(crypto),
  ok = application:start(asn1),
  ok = application:start(public_key),
  ok = application:start(ssl),
  ok = application:start(ranch),
  application:start(cowlib),
  ok = application:start(cowboy),
  ok = application:start(pulsedb),

  application:load(lager),
  application:set_env(lager,crash_log,undefined),

  LogDir = "log",
  ConsoleFormat = [time, " ", pid, {pid, [" "], ""},
    {module, [module, ":", line, " "], ""},
    message, "\n"
  ],
  FileFormat = [date, " "] ++ ConsoleFormat,
  application:set_env(lager,handlers,[
    {lager_console_backend,[info,{lager_default_formatter, ConsoleFormat}]},
    {lager_file_backend, [{file,LogDir++"/pulsedb.log"},{level,info},{size,2097152},{date,"$D04"}, {count,40},
                        {formatter,lager_default_formatter},{formatter_config,FileFormat}]}
  ]),
  ok = lager:start(),

  case application:get_env(pulsedb, path) of
    {ok, Path} ->
      lager:info("Open pulsedb storage at ~s", [Path]),
      Spec = {simple_db, {pulsedb, open, [simple_db, [{url,"file://"++Path}]]}, permanent, 100, worker, []},
      supervisor:start_child(pulsedb_sup, Spec);
    _ ->
      ok
  end,

  Auth = case application:get_env(pulsedb, key) of
    {ok, Key} ->
      NetAuth = [{key,iolist_to_binary(Key)}],
      [{auth,pulsedb_netpush_auth,NetAuth}];
    _ -> 
      []
  end,

  StaticDir = case application:get_key(cowboy,vsn) of
    {ok, "0.9."++_} -> {dir, "webroot/js", [{mimetypes, cow_mimetypes, web}]};
    {ok, "0.8."++_} -> [{directory, "webroot/js"}]
  end,

  Dispatch = [{'_', [
    {"/api/v1/pulse_push", pulsedb_netpush_handler, [{db,simple_db}] ++ Auth},
    {"/embed/[...]", pulsedb_graphic_handler, [{db,simple_db}, {resolver, {pulsedb_graphic_handler, resolve_embed}}]},
    {"/js/[...]", cowboy_static, StaticDir}
  ]}],

  case application:get_env(pulsedb, port) of
    {ok, Port} ->
      KeyInfo = case application:get_env(pulsedb, key) of
        {ok, Key_} -> io_lib:format("using key '~s'", [Key_]);
        _ -> "without any key"
      end,
      lager:info("Start HTTP listener at port ~B ~s", [Port, KeyInfo]),
      ranch:start_listener(pulsedb_service, 1, ranch_tcp, [{port,Port}], cowboy_protocol, [{env, [
        {dispatch, cowboy_router:compile(Dispatch)}
      ]}]);
    _ ->
      undefined
  end.
