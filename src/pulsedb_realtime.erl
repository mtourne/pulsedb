-module(pulsedb_realtime).
-export([subscribe/2, unsubscribe/1]).
-export([start_link/0, init/1, terminate/2, handle_info/2, handle_cast/2]).

-record(subscriptions, 
 {
  clients=[] % query_key, {Name,Query}, pids
  }).

subscribe(Query, Tag) ->
  gen_server:cast(?MODULE, {subscribe, self(), Query, Tag}).


unsubscribe(_) ->
  {error, not_implemented}.


start_link() -> 
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
  erlang:send_after(0, self(), tick),
  {ok, #subscriptions{}}.


terminate(_,_) ->
  ok.


handle_cast({subscribe, Pid, Query, Tag}, #subscriptions{clients=Clients0}=State) ->
  erlang:monitor(process, Pid),
  Clients = case lists:keytake(Query, 1, Clients0) of
    {value, {Q, UTC, Pids0}, Rest} ->
      Pids = lists:usort([{Pid,Tag}|Pids0]),
      [{Q,UTC, Pids} | Rest];
    false ->
      [{Query,undefined,[{Pid,Tag}]} | Clients0]
  end,
  
  {noreply, State#subscriptions{clients=Clients}}.


handle_info(tick, #subscriptions{clients=Clients}=State) ->
  T1 = erlang:now(),
  Clients1 = lists:map(fun({Query,LastUTC,Pids} = Entry) ->
    case pulsedb:read(Query, memory) of
      {ok, [], _} -> 
        Entry;
      {ok, Data, _} ->
        {UTC, V} = lists:last(Data),
        if
          LastUTC == undefined orelse UTC > LastUTC ->
            [Pid ! {pulse, Tag, UTC, V} || {Pid, Tag} <- Pids],
            {Query, UTC, Pids};
          true ->
            Entry
        end                 
    end
  end, Clients),
  Delay = calc_delay(1000, T1),
  erlang:send_after(Delay, self(), tick),
  {noreply, State#subscriptions{clients = Clients1}};



handle_info({'DOWN', _, _, Pid, _}, #subscriptions{clients=Clients0}=State) ->
  Clients = [{Query, UTC, lists:delete(Pid, Pids)} 
             || {Query, UTC, Pids} <- Clients0, 
             Pids =/= [Pid]],
  {noreply, State#subscriptions{clients=Clients}}.


calc_delay(Desired, T1) ->
  case Desired - timer:now_diff(erlang:now(), T1) div 1000 of
    D when D < 0 -> 1;
    D -> D
  end.