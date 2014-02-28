-module(pulsedb_worker).
-export([start_link/2]).
-export([append/2, info/1, info/2, read/3, stop/1, sync/1]).

-export([init/1, handle_info/2, handle_call/3, terminate/2]).



start_link(Name, Options) ->
  proc_lib:start_link(?MODULE, init, [[Name, Options]]).


append(Ticks, DB) when is_pid(DB) ->
  gen_server:call(DB, {append, Ticks, seconds});

append(Ticks, DB) when is_atom(DB) ->
  case whereis(DB) of
    undefined -> undefined;
    Pid -> 
      case append(Ticks, Pid) of 
        {ok, _} -> {ok, DB};
        {error, E} -> {error, E}
      end
  end.


sync(DB) when is_pid(DB) ->
  gen_server:call(DB, sync);

sync(DB) when is_atom(DB) ->
  case whereis(DB) of
    undefined -> ok;
    Pid -> 
      case sync(Pid) of
        {ok, _} -> {ok, DB};
        {error, E} -> {error, E}
      end
  end.


info(Pid) ->
  info(Pid, seconds).


info(DB, Resolution) when is_atom(DB) ->
  case whereis(DB) of
    undefined -> [];
    Pid -> info(Pid, Resolution)
  end;

info(Pid, Resolution) when is_pid(Pid) ->
  gen_server:call(Pid, {info, Resolution}).



read(Name, Query, DB) ->
  {Now, _} = pulsedb:current_second(),
  From = case lists:keyfind(from, 1, Query) of
    {_,_} -> [];
    false -> [{from,Now-60}]
  end,
  To = case lists:keyfind(to, 1, Query) of
    {_,_} -> [];
    false -> [{to,Now}]
  end,
  Resolution = seconds,
  gen_server:call(DB, {read, Name, From ++ To ++ Query, Resolution}).



stop(DB) ->
  gen_server:call(DB, stop).

-define(CLEAN, 12*3600*1000).


-record(worker, {
  db_layers,
  stop_timeout,
  stop_timer,
  clean_timer,
  clean_timeout,
                 
  seconds_timer,
  minutes_timer
}).

init([Name, Options0]) ->
  case Name of
    undefined -> ok;
    _ -> erlang:register(Name, self())
  end,
  
  Resolutions0 = proplists:get_value(resolutions, Options0, [seconds]),
  Resolutions = lists:usort([seconds|Resolutions0]),
  lager:info("Resolutions ~p", [Resolutions]),
  Options = proplists:delete(resolutions, Options0),
  
  DBs0 = [pulsedb:open(undefined, [{resolution, R}|Options]) || R <- Resolutions],
  DBs1 = lists:zip(Resolutions, DBs0),
  DBs = [{R, DB} || {R, {ok, DB}} <- DBs1],
  
  case DBs of
    _ when length(DBs) =/= length(Resolutions) ->
      proc_lib:init_ack({error, proplists:get_all_values(error, DBs)}),
      ok;
    _ -> 
      CleanTimeout = proplists:get_value(delete_older, Options),
      CleanTimer = case CleanTimeout of
        undefined -> undefined;
        CleanTimeout -> erlang:send_after(?CLEAN, self(), clean)
      end,
      proc_lib:init_ack({ok, self()}),
      StopTimeout = proplists:get_value(timeout, Options),
      StopTimer = case StopTimeout of
        undefined -> undefined;
        _ -> erlang:send_after(StopTimeout, self(), stop)
      end,
      State0 = #worker{db_layers = DBs, clean_timer = CleanTimer, clean_timeout = CleanTimeout, stop_timeout = StopTimeout, stop_timer = StopTimer},
      State = init_timers(Resolutions, State0),
      gen_server:enter_loop(?MODULE, [], State)
  end.

init_timers(Resolutions, #worker{}=W) -> 
  case lists:member(minutes, Resolutions) of
    true ->
      W1 = start_timer(seconds, W),
      case lists:member(hours, Resolutions) of
        true -> start_timer(minutes, W1);
        _ -> W1
      end;
    _ -> 
      W
  end.
      


handle_call({append, Ticks, Resolution}, _, #worker{db_layers = DBs, stop_timeout = StopTimeout, stop_timer = OldTimer} = W) ->
  DB = find_db(Resolution, DBs),
  case pulsedb:append(Ticks, DB) of
    {ok, DB1} ->
      StopTimer = case StopTimeout of
        undefined -> undefined;
        _ ->
          erlang:cancel_timer(OldTimer),
          erlang:send_after(StopTimeout, self(), stop)
      end,
      DBs1 = update_db(Resolution, DB1, DBs),
      {reply, {ok, self()}, W#worker{db_layers = DBs1, stop_timer = StopTimer, stop_timeout = StopTimeout}};
    {error, E} ->
      {stop, normal, {error, E}, W}
  end;


handle_call({read, Name, Query, Resolution}, _, #worker{db_layers = DBs} = W) ->
  DB = find_db(Resolution, DBs),
  {ok, Ticks, DB2} = pulsedb:read(Name, Query, DB),
  DBs1 = update_db(Resolution, DB2, DBs),
  {reply, {ok, Ticks, self()}, W#worker{db_layers = DBs1}};


handle_call({info, Resolution}, _, #worker{db_layers = DBs} = W) ->
  DB = find_db(Resolution, DBs),
  {reply, pulsedb:info(DB), W};


handle_call(sync, _, #worker{db_layers = DBs} = W) ->
  DBs1 = [begin
            {ok, DB1} = pulsedb:sync(DB),
            {Res, DB1} 
            end || {Res, DB} <- DBs],
  {reply, {ok, self()}, W#worker{db_layers = DBs1}};


handle_call(stop, _, #worker{} = W) ->
  {stop, normal, ok, W}.




handle_info(stop, #worker{} = W) ->
  {stop, normal, W};


handle_info(clean, #worker{clean_timer = undefined} = W) ->
  {noreply, W};


handle_info(clean, #worker{clean_timer = OldTimer, clean_timeout = Timeout, db_layers = DBs} = W) ->
  erlang:cancel_timer(OldTimer),
  Resolution = seconds,
  DB = find_db(Resolution, DBs),
  Module = element(2,DB),
  {ok, DB1} = Module:delete_older(Timeout, DB),
  DBs1 = update_db(Resolution, DB1, DBs),
  Timer = erlang:send_after(?CLEAN, self(), clean),
  {noreply, W#worker{clean_timer = Timer, db_layers = DBs1}};


handle_info({aggregate, Resolution}, #worker{seconds_timer = Timer0, db_layers = DBs}=W) ->
  Timer0 = timer_for(Resolution, W),
  erlang:cancel_timer(Timer0),

  TargetResolution = next_layer(Resolution, DBs),
  W1 =
  case TargetResolution of 
    undefined -> 
      W;
    _ ->
      Source0 = find_db(Resolution, DBs),
      Target0 = find_db(TargetResolution, DBs),
  
      Info = pulsedb:info(Source0),
      Metrics = proplists:get_value(sources, Info, []),
  
      Target1 = lists:foldl(fun({Name, Tags}, T0) ->
        Data = block_data(Name, Tags, TargetResolution, Source0),
        TicksAvg = [{<<"avg-", Name/binary>>, UTC, Value, Tags} || {UTC, Value} <- proplists:get_value(avg, Data, [])],
        TicksMax = [{<<"max-", Name/binary>>, UTC, Value, Tags} || {UTC, Value} <- proplists:get_value(max, Data, [])],

        {ok, T1} = pulsedb:append(TicksAvg, T0),
        {ok, T2} = pulsedb:append(TicksMax, T1),
        T2
      end, Target0, Metrics),
      DBs1 = update_db(TargetResolution, Target1, DBs),
      W#worker{db_layers = DBs1}
  end,

  W2 = start_timer(Resolution, W1),
  {noreply, W2}.


terminate(_,_) ->
  ok.


next_layer(seconds, DBs) -> 
  case proplists:is_defined(minutes, DBs) of
    true -> minutes;
    _ -> undefined
  end;

next_layer(minutes, DBs) ->
  case proplists:is_defined(hours, DBs) of 
    true -> hours;
    _ -> undefined
  end;

next_layer(_,_) -> 
  undefined.



find_db(Resolution, Layers) ->
  proplists:get_value(Resolution, Layers).

update_db(Resolution, DB, Layers0) ->
  Layers1 = proplists:delete(Resolution, Layers0),
  [{Resolution, DB}|Layers1].


start_timer(seconds, #worker{}=W) ->
  {_,Delay} = pulsedb:current_minute(),
  lager:info("AGGREGATION: ~p NEXT START AFTER ~p", [seconds, Delay]),
  Timer = erlang:send_after(Delay, self(), {aggregate, seconds}),
  W#worker{seconds_timer = Timer};

start_timer(minutes, #worker{}=W) ->
  {_,Delay} = pulsedb:current_hour(),
  Timer = erlang:send_after(Delay, self(), {aggregate, minutes}),
  W#worker{minutes_timer = Timer}.



timer_for(seconds, #worker{seconds_timer=T}) -> T;
timer_for(minutes, #worker{minutes_timer=T}) -> T.



block_data(Name, Tags, Resolution, Source) ->
  {Now, _} = pulsedb:current_second(),
  From = first_utc(Now, Resolution),
  To = last_utc(Now, Resolution),
  UTCPeriod = To - From + 1,
  
  Query = [{aggregator, <<"sum">>}, {from, From}, {to, To} | Tags],
  {ok, Ticks, _} = pulsedb:read(Name, Query, Source),

  Downsamplers = 
  case Name of
    <<"avg-",_/binary>> -> [avg];
    <<"max-",_/binary>> -> [max];
    _ -> [avg, max]
  end,
  
  downsample(Downsamplers, From, UTCPeriod, Ticks).
  

downsample([],_,_,_) -> [];
downsample([Ds|Rest], From, UTCPeriod, Ticks) when is_atom(Ds) ->
  DsBin = atom_to_binary(Ds, latin1),
  Downsampled = pulsedb_data:downsample({UTCPeriod, DsBin}, Ticks),
  Value = proplists:get_value(From, Downsampled),
  case Value of
    undefined -> [];
    _ -> [{Ds, [{From, Value}]}
          | downsample(Rest, From, UTCPeriod, Ticks)]
  end.


%%%%%%%%%%%%%%
% TIME HELPERS
%%%%%%%%%%%%%%

first_utc(UTC, minutes) -> ((UTC div 60) - 1) * 60;
first_utc(UTC, hours)   -> ((UTC div 3600) - 1) * 3600.
  
last_utc(UTC, minutes) -> ((UTC div 60)) * 60 - 1;
last_utc(UTC, hours)   -> ((UTC div 3600)) * 3600 - 1.
