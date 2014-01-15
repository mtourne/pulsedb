-module(pulsedb_collector).
-export([start_link/3]).
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



start_link(Name, Module, Args) ->
  gen_server:start_link(?MODULE, [Name, Module, Args], []).





stop(Name) ->
  supervisor:terminate_child(pulse_flows, Name),
  supervisor:delete_child(pulse_flows, Name),

  ets:select_delete(pulse_flow_minute, ets:fun2ms(fun(Row) when 
    element(1,element(1,Row)) == Name  -> true
  end)),

  ets:select_delete(pulse_flow_second, ets:fun2ms(fun(Row) when 
    element(1,element(1,Row)) == Name -> true
  end)),
  ets:delete(pulse_flow_config, Name),
  ok.




-record(collect, {
  name :: pulsedb:name(),
  collect_timer = undefined :: undefined | term(),
  flush_timer :: term(),
  module = undefined :: undefined | atom(),
  known_metrics = [],
  state :: term()
}).


init([Name, Module, Args]) ->
  {_, FlushDelay} = pulsedb:current_minute(),
  FlushTimer = erlang:send_after(FlushDelay, self(), flush),

  put(name, {pulse_collector,Name,Module}),

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
  {ok, #collect{name = Name, flush_timer = FlushTimer,
    collect_timer = CollectTimer, module = Module, state = State}}.


handle_call(stop, _, #collect{} = Flow) ->
  {stop, normal, ok, Flow};
handle_call(Call, _, #collect{} = C) ->
  {stop, {error, {unknown_call, Call}}, C}.



handle_info(collect, #collect{state = State,
  collect_timer = OldTimer, module = Module, known_metrics = KnownMetrics} = Flow) ->
  erlang:cancel_timer(OldTimer),
  {UTC, CollectDelay} = pulsedb:current_second(),

  {reply, Values, State1} = Module:pulse_collect(State),
  Ticks = [{Name,UTC,Value,Tags} || {Name,Value,Tags} <- Values],

  % FIXME: here we save data from pulsedb_collector _only_ to memory
  pulsedb_memory:append(Ticks, seconds),

  Metrics = lists:foldl(fun({Name,_,Tags}, List) ->
    case lists:keyfind({Name,Tags},List) of
      false -> [{{Name,Tags}, pulsedb_disk:metric_name(Name,Tags)}|List];
      _ -> List
    end
  end, KnownMetrics, Values),
  CollectTimer = erlang:send_after(CollectDelay, self(), collect),
  {noreply, Flow#collect{collect_timer = CollectTimer, state = State1, known_metrics = Metrics}};



handle_info(flush, #collect{flush_timer = OldTimer, known_metrics = Metrics} = Flow) ->
  erlang:cancel_timer(OldTimer),
  {UTC, FlushDelay} = pulsedb:current_minute(),


  _Updates = pulsedb_memory:merge_seconds_data([ {Name,Tag} || {{Name,Tag},_} <- Metrics], UTC),

  % FIXME: send Updates to clients
  % [Pid ! Msg || {_,Pid} <- ets:lookup(pulse_flow_clients, Name)],
  % [Pid ! Msg || {_,Pid} <- ets:lookup(pulse_flow_clients, all)],

  FlushTimer = erlang:send_after(FlushDelay, self(), flush),
  {noreply, Flow#collect{flush_timer = FlushTimer}}.




terminate(_,_) ->
  ok.










