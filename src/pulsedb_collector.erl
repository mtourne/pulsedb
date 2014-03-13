-module(pulsedb_collector).
-export([start_link/4]).
-export([stop/1]).
-export([list/0]).
-export([init/1, handle_call/3, handle_info/2, terminate/2]).
-include_lib("stdlib/include/ms_transform.hrl").



-callback pulse_init(Arg::term()) -> 
  {ok, State::term()}.

-callback pulse_collect(Arg::term()) -> 
  {reply, [{Name::pulsedb:name(),UTC::non_neg_integer(), Value::non_neg_integer(), Tags::list()}], Arg1::term()}.


  

-record(collect_config, {
  name
}).



list() ->
  [Name || #collect_config{name = Name} <- ets:tab2list(pulsedb_collectors)].



start_link(Name, Module, Args, Options) ->
  gen_server:start_link(?MODULE, [Name, Module, Args, Options], []).





stop(Name) ->
  Metrics = case lists:keyfind(Name, 1, supervisor:which_children(pulsedb_collectors)) of
    {Name, Pid, _, _} -> gen_server:call(Pid, metrics);
    _ -> []
  end,

  supervisor:terminate_child(pulsedb_collectors, Name),
  supervisor:delete_child(pulsedb_collectors, Name),

  lists:foreach(fun(M) ->
    pulsedb_memory:clean_data(M)
  end, Metrics),
  ok.




-record(collect, {
  name :: pulsedb:name(),
  collect_timer = undefined :: undefined | term(),
  copy_to,
  flush_timer :: term(),
  module = undefined :: undefined | atom(),
  known_metrics = [],
  state :: term()
}).


init([Name, Module, Args, Options]) ->
  {_, FlushDelay} = pulsedb:current_minute(),
  FlushTimer = erlang:send_after(FlushDelay, self(), flush),

  put(name, {pulsedb_collector,Name,Module}),

  Module:module_info(),

  State = case erlang:function_exported(Module, pulse_init, 1) of
    true -> 
      case Module:pulse_init(Args) of
        {ok, State_} -> State_;
        Else -> error({bad_return,{Module,pulse_init,Args}, Else})
      end;
    false -> Args
  end,

  {_, CollectDelay} = pulsedb:current_second(),
  CollectTimer = erlang:send_after(CollectDelay, self(), collect),
  ets:insert(pulsedb_collectors, #collect_config{name = Name}),
  Copy = proplists:get_value(copy, Options),
  {ok, #collect{name = Name, flush_timer = FlushTimer, copy_to = Copy,
    collect_timer = CollectTimer, module = Module, state = State}}.



handle_call(metrics, _, #collect{known_metrics = Metrics} = Flow) ->
  {reply, [M || {_,M} <- Metrics], Flow};

handle_call(stop, _, #collect{} = Flow) ->
  {stop, normal, ok, Flow};
handle_call(Call, _, #collect{} = C) ->
  {stop, {error, {unknown_call, Call}}, C}.



handle_info(collect, #collect{collect_timer = OldTimer, name = Name} = Flow) ->
  erlang:cancel_timer(OldTimer),
  {UTC, CollectDelay} = pulsedb:current_second(),

  Flow1 = try collect(Flow, UTC)
  catch
    C:E ->
      ST = erlang:get_stacktrace(),
      lager:info("Failed to collect pulse ~s: ~p:~p\n~p", [Name, C, E, ST]),
      Flow
  end,

  CollectTimer = erlang:send_after(CollectDelay, self(), collect),
  {noreply, Flow1#collect{collect_timer = CollectTimer}};



handle_info(flush, #collect{flush_timer = OldTimer, known_metrics = Metrics} = Flow) ->
  erlang:cancel_timer(OldTimer),
  {UTC, FlushDelay} = pulsedb:current_minute(),

  % FIXME: need to move minute aggregation to pulsedb_memory from here
  _Updates = pulsedb_memory:merge_seconds_data([ {Name,Tag} || {{Name,Tag},_} <- Metrics], (UTC div 60)*60),

  % FIXME: send Updates to clients
  % [Pid ! Msg || {_,Pid} <- ets:lookup(pulse_flow_clients, Name)],
  % [Pid ! Msg || {_,Pid} <- ets:lookup(pulse_flow_clients, all)],

  FlushTimer = erlang:send_after(FlushDelay, self(), flush),
  {noreply, Flow#collect{flush_timer = FlushTimer}};

handle_info(_, #collect{} = Flow) ->
  {noreply, Flow}.



to_b(Bin) when is_binary(Bin) -> Bin;
to_b(Atom) when is_atom(Atom) -> atom_to_binary(Atom,latin1).



terminate(_,_) ->
  ok.




collect(#collect{state = State, copy_to = Copy, module = Module, known_metrics = KnownMetrics} = Flow, UTC) ->
  {reply, Values, State1} = Module:pulse_collect(State),
  Ticks = [{to_b(Name),UTC,Value,[{to_b(K),to_b(V)} || {K,V} <- Tags]} || {Name,Value,Tags} <- Values],

  pulsedb_memory:append(Ticks, seconds),
  case Copy of
    undefined -> ok;
    _ -> pulsedb:append(Ticks, Copy)
  end,

  Metrics = lists:foldl(fun({Name,_,_,Tags}, List) ->
    case lists:keyfind({Name,Tags},1,List) of
      false -> [{{Name,Tags}, pulsedb_disk:metric_name(Name,Tags)}|List];
      _ -> List
    end
  end, KnownMetrics, Ticks),
  Flow#collect{state = State1, known_metrics = Metrics}.







