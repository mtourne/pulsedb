-module(pulsedb_memory).
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_info/2, terminate/2]).

-export([append/2, read/3]).
-export([merge_seconds_data/2]).


-export([subscribe/2, unsubscribe/2]).
-export([replicate/2]).
-export([info/1]).
-export([clean_data/1]).




start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).



replicate(DB, Pid) ->
  gen_server:call(?MODULE, {replicate, DB, Pid}).



append(Ticks, DB) when is_list(Ticks) ->
  [append0(Tick, DB) || Tick <- Ticks],
  {ok, DB};

append(Tick, DB) when is_tuple(Tick) ->
  append0(Tick, DB),
  {ok, DB}.


append0(Tuple={Name,UTC,Value,Tags}, DB) ->
  Table = table(DB),
  Metric = cached_metric_name(Name,Tags, DB),
  ets:insert_new(Table, {{Metric,UTC}, Value}),
  Pulse = {pulse,DB,Name,UTC,Value,Tags},
  [Pid ! Pulse || {_,Pid} <- ets:lookup(pulsedb_replicators, DB)],
  ok.


cached_metric_name(Name, Tags, DB) ->
  case ets:lookup(metric_table(DB), {Name, Tags}) of
    [{_, Metric}] ->
      Metric;
    [] ->
      Metric = pulsedb_disk:metric_name(Name, Tags),
      ets:insert(metric_table(DB), {{Name,Tags},Metric}),
      Metric
  end.



%% XX (mtourne) maybe it could be 3 different
%% calls for milli, secs, minutes.

%% TODO (mtourne): implement milliseconds query.
read(Name, Query, DB) when DB == seconds orelse DB == minutes ->
  Tags = [{K,V} || {K,V} <- Query, is_binary(K)],

  Metrics = [M || {{N,T},M} <- ets:tab2list(metric_table(DB)), N == Name andalso pulsedb_disk:metric_fits_query(Tags,T)],

  Table = table(DB),
  Step = step(DB),

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

  Values2 = pulsedb_data:aggregate(proplists:get_value(aggregator,Query), lists:sort(Values1)),
  Values3 = pulsedb_data:downsample(proplists:get_value(downsampler,Query), Values2),

  {ok, Values3, DB}.


%% XX (mtourne): is this used by anyone ?
merge_seconds_data(Metrics, UTC) when UTC rem 60 == 0 ->
  Ticks = lists:seq(UTC - 60, UTC, 1),

  Updates = lists:flatmap(fun({Name,Tags}) ->
    Metric = cached_metric_name(Name, Tags, seconds),
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
        Value = lists:sum(Stats) div 60,
        Tick = {Name, UTC-60, Value, Tags},
        append(Tick, minutes),
        [Tick]
    end
  end, Metrics),
  Updates.




info(DB) ->
  Table = metric_table(DB),
  [{sources, lists:usort([Q || {Q, _} <- ets:tab2list(Table)])}].

table(milliseconds) -> pulsedb_milliseconds_data;
table(seconds) -> pulsedb_seconds_data;
table(minutes) -> pulsedb_minutes_data.

metric_table(milliseconds) -> pulsedb_metric_names_milliseconds;
metric_table(seconds) -> pulsedb_metric_names_seconds;
metric_table(minutes) -> pulsedb_metric_names_minutes.

step(seconds) -> 1;
step(minutes) -> 60.




clean_data(MetricName) ->
  ets:select_delete(pulsedb_seconds_data, ets:fun2ms(fun(Row) when
    element(1,element(1,Row)) == MetricName -> true
  end)),

  ets:select_delete(pulsedb_minutes_data, ets:fun2ms(fun(Row) when
    element(1,element(1,Row)) == MetricName  -> true
  end)),

  ets:select_delete(pulsedb_metric_names_seconds, ets:fun2ms(fun({_,M_}) when
    M_ == MetricName  -> true
  end)),

  ets:select_delete(pulsedb_metric_names_minutes, ets:fun2ms(fun({_,M_}) when
    M_ == MetricName  -> true
  end)).




subscribe(Pid, Query) ->
  gen_server:call(?MODULE, {subscribe, Pid, Query}).


unsubscribe(Pid, Query) ->
  gen_server:call(?MODULE, {unsubscribe, Pid, Query}).

-record(storage, {
  clean_timer
}).

%% 100,000 ms run once every 100 secs.
-define(CLEANUP_INTERVAL, 100000).

init([]) ->
  ets:new(pulsedb_collectors, [public, named_table, {keypos,2}, {read_concurrency,true}]),
  ets:new(pulsedb_milliseconds_data, [public, named_table, {write_concurrency,true}]),
  ets:new(pulsedb_seconds_data, [public, named_table, {write_concurrency,true}]),
  ets:new(pulsedb_minutes_data, [public, named_table, {write_concurrency,true}]),

  ets:new(pulsedb_metric_names_milliseconds,
          [public, named_table, {write_concurrency,true}]),
  ets:new(pulsedb_metric_names_seconds, [public, named_table, {read_concurrency,true}]),
  ets:new(pulsedb_metric_names_minutes, [public, named_table, {read_concurrency,true}]),

  ets:new(pulsedb_replicators,  [public, named_table, {read_concurrency,true}, bag]),
  % ets:new(pulse_flow_clients, [public, named_table, bag, {read_concurrency,true}]),

  %% Note (mtourne): erlang:send_after is more efficient than timer:send_after
  %% http://www.erlang.org/doc/efficiency_guide/commoncaveats.html#id2262042
  CleanTimer = erlang:send_after(?CLEANUP_INTERVAL, self(), clean),
  {ok, #storage{clean_timer = CleanTimer}}.


% handle_call({subscribe,Pid,Name}, _, #storage{} = S) ->
%   MS = ets:fun2ms(fun({N,P}) -> N == Name andalso P == Pid end),
%   case ets:select_count(pulse_flow_clients, MS) of
%     1 -> {reply, ok, S};
%     0 ->
%       erlang:monitor(process, Pid),
%       ets:insert(pulse_flow_clients, {Name,Pid}),
%       {reply, ok, S}
%   end;
%
% handle_call({unsubscribe,Pid,Name}, _, #storage{} = S) ->
%   ets:delete_object(pulse_flow_clients, {Name,Pid}),
%   {reply, ok, S};


handle_call({replicate, DB, Pid}, _, #storage{} = S) ->
  ets:insert(pulsedb_replicators, {DB, Pid}),
  erlang:monitor(process, Pid),
  {reply, ok, S}.




handle_info(clean, #storage{clean_timer = Old} = S) ->
  erlang:cancel_timer(Old),
  {Minute, _} = pulsedb:current_minute(),

  %% Cleanup everything older than 5 hours
  ets:select_delete(pulsedb_minutes_data, ets:fun2ms(fun(Row) when
    element(2,element(1,Row)) < Minute - 5*3600 ->
    true
  end)),

  %% Cleanup everything older than 2 minutes
  ets:select_delete(pulsedb_seconds_data, ets:fun2ms(fun(Row) when
    element(2,element(1,Row)) < Minute - 120 ->
    true
  end)),

  %% Cleanup everything older than 1 minute
  ets:select_delete(pulsedb_milliseconds_data, ets:fun2ms(fun(Row) when
    element(2,element(1,Row)) < ((Minute - 60) * 1000) ->
    true
  end)),

  Timer = erlang:send_after(?CLEANUP_INTERVAL, self(), clean),
  {noreply, S#storage{clean_timer = Timer}};





handle_info({'DOWN', _, _, Pid, _}, #storage{} = S) ->
  MS = ets:fun2ms(fun({_,P}) -> P == Pid end),
  % ets:select_delete(pulse_flow_clients, MS),
  ets:select_delete(pulsedb_replicators, MS),
  {noreply, S}.



terminate(_,_) ->
  ok.
