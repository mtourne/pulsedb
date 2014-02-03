-module(pulsedb_worker).
-export([start_link/2]).
-export([append/2, info/1, read/3, stop/1, sync/1]).

-export([init/1, handle_info/2, handle_call/3, terminate/2]).



start_link(Name, Options) ->
  proc_lib:start_link(?MODULE, init, [[Name, Options]]).


append(Ticks, DB) when is_pid(DB) ->
  gen_server:call(DB, {append, Ticks});

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


info(DB) when is_atom(DB) ->
  case whereis(DB) of
    undefined -> [];
    Pid -> info(Pid)
  end;

info(Pid) when is_pid(Pid) ->
  gen_server:call(Pid, info).




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
  gen_server:call(DB, {read, Name, From ++ To ++ Query}).



stop(DB) ->
  gen_server:call(DB, stop).

-define(CLEAN, 12*3600*1000).


-record(worker, {
  db,
  clean_timer,
  clean_timeout
}).

init([Name, Options]) ->
  case Name of
    undefined -> ok;
    _ -> erlang:register(Name, self())
  end,
  case pulsedb:open(undefined, Options) of
    {ok, DB} ->
      CleanTimeout = proplists:get_value(delete_older, Options),
      CleanTimer = case CleanTimeout of
        undefined -> undefined;
        CleanTimeout -> erlang:send_after(?CLEAN, self(), clean)
      end,
      proc_lib:init_ack({ok, self()}),
      State = #worker{db = DB, clean_timer = CleanTimer, clean_timeout = CleanTimeout},
      gen_server:enter_loop(?MODULE, [], State);
    {error, E} ->
      proc_lib:init_ack({error, E}),
      ok
  end.


handle_call({append, Ticks}, _, #worker{db = DB} = W) ->
  case pulsedb:append(Ticks, DB) of
    {ok, DB1} ->
      {reply, {ok, self()}, W#worker{db = DB1}};
    {error, E} ->
      {stop, normal, {error, E}, W}
  end;

handle_call({read, Name, Query}, _, #worker{db = DB} = W) ->
  {ok, Ticks, DB2} = pulsedb:read(Name, Query, DB),
  {reply, {ok, Ticks, self()}, W#worker{db = DB2}};

handle_call(info, _, #worker{db = DB} = W) ->
  {reply, pulsedb:info(DB), W};

handle_call(sync, _, #worker{db = DB} = W) ->
  {ok, DB1} = pulsedb:sync(DB),
  {reply, {ok, self()}, W#worker{db = DB1}};

handle_call(stop, _, #worker{} = W) ->
  {stop, normal, ok, W}.


handle_info(clean, #worker{clean_timer = undefined} = W) ->
  {noreply, W};

handle_info(clean, #worker{clean_timer = OldTimer, clean_timeout = Timeout, db = DB} = W) ->
  erlang:cancel_timer(OldTimer),

  Module = element(2,DB),
  {ok, DB1} = Module:delete_older(Timeout, DB),

  Timer = erlang:send_after(?CLEAN, self(), clean),
  {noreply, W#worker{clean_timer = Timer, db = DB1}}.



terminate(_,_) ->
  ok.

