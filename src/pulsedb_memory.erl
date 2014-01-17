-module(pulsedb_memory).
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_info/2, terminate/2]).

-export([append/2, read/3]).
-export([merge_seconds_data/2]).


-export([subscribe/2, unsubscribe/2]).




start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


append(Ticks, DB) when is_list(Ticks) ->
  [append0(Tick, DB) || Tick <- Ticks],
  ok;

append(Tick, DB) when is_tuple(Tick) ->
  append0(Tick, DB),
  ok.


append0({Name,UTC,Value,Tags}, DB) ->
  Table = case DB of
    seconds -> pulsedb_seconds_data;
    minutes -> pulsedb_minutes_data
  end,
  Metric = cached_metric_name(Name,Tags),
  ets:insert_new(Table, {{Metric,UTC}, Value}),
  ok.


cached_metric_name(Name, Tags) ->
  case ets:lookup(pulsedb_metric_names, {Name, Tags}) of
    [{_, Metric}] -> 
      Metric;
    [] ->
      Metric = pulsedb_disk:metric_name(Name, Tags),
      ets:insert(pulsedb_metric_names, {{Name,Tags},Metric}),
      Metric
  end.




read(Name, Query, DB) when DB == seconds orelse DB == minutes ->
  Tags = [{K,V} || {K,V} <- Query, is_binary(K)],

  Metrics = [M || {{N,T},M} <- ets:tab2list(pulsedb_metric_names), N == Name andalso pulsedb_disk:metric_fits_query(Tags,T)],

  Table = case DB of
    seconds -> pulsedb_seconds_data;
    minutes -> pulsedb_minutes_data
  end,

  Step = case DB of
    seconds -> 1;
    minutes -> 60
  end,


  {Now,_} = pulsedb:current_second(),
  From_ = case lists:keyfind(from, 1, Query) of
    {_, From1_} -> From1_;
    false when DB == seconds -> Now - 60;
    false when DB == minutes -> Now - 4*3600
  end,

  To_ = case lists:keyfind(to, 1, Query) of
    {_, To1_} -> To1_;
    false -> Now
  end,

  From = (From_ div Step)*Step,
  To = (To_ div Step)*Step,

  Ticks = lists:seq(From, To, Step),

  Values1 = lists:flatmap(fun(Metric) ->
    lists:flatmap(fun(I) ->
      case ets:lookup(Table, {Metric, I}) of
        [{{_,_},Value}] -> [{I,Value}];
        [] -> []
      end
    end, Ticks)
  end, Metrics),

  Values2 = pulsedb_disk:aggregate(proplists:get_value(aggregator,Query), lists:sort(Values1)),
  Values3 = pulsedb_disk:downsample(proplists:get_value(downsampler,Query), Values2),

  {ok, Values3, DB}.



merge_seconds_data(Metrics, UTC) when UTC rem 60 == 0 ->
  Ticks = lists:seq(UTC - 60, UTC, 1),

  Updates = lists:flatmap(fun({Name,Tags}) ->
    Metric = cached_metric_name(Name, Tags),
    Stats = lists:flatmap(fun(I) ->
      case ets:lookup(pulsedb_seconds_data, {Metric,I}) of
        [{_,Value}] -> [Value];
        [] -> []
      end
    end, Ticks),
    case Stats of
      [] -> 
        [];
      _ ->
        Value = lists:sum(Stats) div length(Stats),
        ets:insert(pulsedb_minutes_data, {{Metric,UTC - 60}, Value}),
        [{Name,UTC - 60,Value, Tags}]
    end
  end, Metrics),
  Updates.







subscribe(Pid, Query) ->
  gen_server:call(?MODULE, {subscribe, Pid, Query}).


unsubscribe(Pid, Query) ->
  gen_server:call(?MODULE, {unsubscribe, Pid, Query}).

-record(storage, {
  clean_timer
}).

-define(TIMEOUT, 100000).

init([]) ->
  ets:new(pulsedb_collectors, [public, named_table, {keypos,2}, {read_concurrency,true}]),
  ets:new(pulsedb_seconds_data, [public, named_table, {write_concurrency,true}]),
  ets:new(pulsedb_minutes_data, [public, named_table, {write_concurrency,true}]),
  ets:new(pulsedb_metric_names, [public, named_table, {read_concurrency,true}]),
  % ets:new(pulse_flow_clients, [public, named_table, bag, {read_concurrency,true}]),

  CleanTimer = erlang:send_after(?TIMEOUT, self(), clean),
  {ok, #storage{clean_timer = CleanTimer}}.


handle_call({subscribe,Pid,Name}, _, #storage{} = S) ->
  MS = ets:fun2ms(fun({N,P}) -> N == Name andalso P == Pid end),
  case ets:select_count(pulse_flow_clients, MS) of
    1 -> {reply, ok, S};
    0 ->
      erlang:monitor(process, Pid),
      ets:insert(pulse_flow_clients, {Name,Pid}),
      {reply, ok, S}
  end;

handle_call({unsubscribe,Pid,Name}, _, #storage{} = S) ->
  ets:delete_object(pulse_flow_clients, {Name,Pid}),
  {reply, ok, S}.






handle_info(clean, #storage{clean_timer = Old} = S) ->
  erlang:cancel_timer(Old),
  {Minute, _} = pulsedb:current_minute(),

  ets:select_delete(pulsedb_minutes_data, ets:fun2ms(fun(Row) when 
    element(2,element(1,Row)) < Minute - 5*3600 ->
    true
  end)),

  ets:select_delete(pulsedb_seconds_data, ets:fun2ms(fun(Row) when 
    element(2,element(1,Row)) < Minute - 60 ->
    true
  end)),

  Timer = erlang:send_after(?TIMEOUT, self(), clean),
  {noreply, S#storage{clean_timer = Timer}};





handle_info({'DOWN', _, _, Pid, _}, #storage{} = S) ->
  MS = ets:fun2ms(fun({_,P}) -> P == Pid end),
  ets:select_delete(pulse_flow_clients, MS),
  {noreply, S}.



terminate(_,_) ->
  ok.



