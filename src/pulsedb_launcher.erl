-module(pulsedb_launcher).

-export([start/0]).
-export([pushers/0]).
-export([workers/0]).

workers() ->
  [begin
    {_,Dict} = process_info(Pid,dictionary),
    {Type, IP} = case proplists:get_value(name,Dict) of
      {pulsedb_netpush,I} -> {pusher,I};
      {pulsedb_graph,I} -> {graphic,I};
      _ -> {undefined,undefined}
    end,
    {Pid,Type,IP}
  end || {_,Pid,_,_} <-  supervisor:which_children(ranch_server:get_connections_sup(pulsedb_service))].


pushers() ->
  [{Pid,Ip} || {Pid,pusher,Ip} <- workers()].

start() ->
  application:load(pulsedb),

  case file:path_consult(["priv", "/etc/pulsedb"], "pulsedb.config") of
    {ok, Env, ConfigPath} ->
      error_logger:info_msg("Reading config from ~s\n", [ConfigPath]),
      [application:set_env(pulsedb, K, V) || {K,V} <- Env];
    {error, ConfigError} ->
      error_logger:error_msg("Failed to read config: ~p\n", [ConfigError]),
      ok
  end,


  ok = application:start(crypto),
  ok = application:start(asn1),
  ok = application:start(public_key),
  ok = application:start(ssl),
  ok = application:start(ranch),
  application:start(cowlib),
  ok = application:start(cowboy),
  ok = application:start(lhttpc),
  ok = application:start(pulsedb),

  application:load(lager),
  application:set_env(lager,crash_log,undefined),

  LogDir = case os:getenv("LOGDIR") of
    false -> "log";
    OsLogDir -> OsLogDir
  end,

  ConsoleFormat = [time, " ", pid, {pid, [" "], ""},
    {module, [module, ":", line, " "], ""},
    message, "\n"
  ],
  FileFormat = [date, " "] ++ ConsoleFormat,
  application:set_env(lager,handlers,[
    {lager_console_backend,[debug,{lager_default_formatter, ConsoleFormat}]},
    {lager_file_backend, [{file,LogDir++"/pulsedb.log"},{level,info},{size,2097152},{date,"$D04"}, {count,40},
                        {formatter,lager_default_formatter},{formatter_config,FileFormat}]}
  ]),

  application:set_env(lager,crash_log,LogDir ++ "/crash.log"),
  application:set_env(lager,crash_log_msg_size,16384),
  application:set_env(lager,crash_log_size,1048576),
  application:set_env(lager,crash_log_date,"$D04"),
  application:set_env(lager,crash_log_count,5),
  application:set_env(lager,error_logger_hwm,300),

  ok = lager:start(),

  lager:info("Starting whole pulsedb server"),

  Auth = case application:get_env(pulsedb, key) of
    {ok, Key} ->
      NetAuth = [{key,iolist_to_binary(Key)}],
      [{auth,pulsedb_netpush_auth,NetAuth}];
    _ ->
      []
  end,

  EmbedResolver = case application:get_env(pulsedb, embed_resolver) of
    {ok, Url} ->
      [{resolver, url, Url}];
    _ ->
      []
  end,

  Superuser = case application:get_env(pulsedb, superuser) of
    {ok, {Login, Pass}} ->
      [{superuser, Login, Pass}];
    _ ->
      []
  end,
  
  
  PulsesDB = case application:get_env(pulsedb, path) of
    {ok, Path} ->
      [{db, {undefined, [{url,"sharded://"++Path},
                         {shard_tag, <<"account">>},
                         {tracker, pulsedb_shards},
                         {timeout,120*1000}, 
                         {resolutions, [seconds, minutes]}]}}];
    _ ->
      []
  end,

  
  StaticDir = case application:get_key(cowboy,vsn) of
    {ok, "0.9."++_} -> {dir, "webroot/js", [{mimetypes, cow_mimetypes, web}]};
    {ok, "0.8."++_} -> [{directory, "webroot/js"}]
  end,

  Dispatch = [{'_', [
    {"/api/v1/status", pulsedb_netpush_handler, [status]},
    {"/api/v1/pulse_push", pulsedb_netpush_handler, PulsesDB ++ Auth},
    {"/embed/[...]", pulsedb_graphic_handler, PulsesDB ++ EmbedResolver ++ Auth},
    {"/pages/points/[...]", pulsedb_points_handler, [] ++ Superuser},
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
      lager:debug("No HTTP port configured, don't start listener"),
      undefined
  end,
  case os:getenv("PIDFILE") of
    false -> ok;
    PidFile ->
      filelib:ensure_dir(PidFile),
      file:write_file(PidFile, os:getpid())
  end,

  ok.
