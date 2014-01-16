-module(pulsedb_realtime).
-export([subscribe/1]).
-export([start_link/0, init/1, terminate/2, handle_info/2, handle_cast/2]).

-record(subscriptions, 
 {
  clients=[] % query_key, {Name,Query}, pids
  }).

subscribe(Query) ->
  gen_server:cast(?MODULE, {subscribe, self(), Query}).
  

start_link() -> 
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
  erlang:send_after(0, self(), tick),
  {ok, #subscriptions{}}.


terminate(_,_) ->
  ok.


handle_cast({subscribe, Pid, Query}, #subscriptions{clients=Clients0}=State) ->
  erlang:monitor(process, Pid),
  {Found, Rest} = lists:partition(fun ({Key0,_}) -> Key0 == Query end, Clients0),
  Clients = case Found of
    [{Q, Pids0}] -> 
      Pids = lists:usort([Pid|Pids0]),
      [{Q,Pids} | Rest];
    _ ->
      [{Query,[Pid]} | Rest]
  end,
  
  {noreply, State#subscriptions{clients=Clients}}.


handle_info(tick, #subscriptions{clients=Clients}=State) ->
  T1 = erlang:now(),
  lists:foreach(fun ({Query,Pids}) ->
                  {ok, Data,_} = pulsedb:read(Query, memory),
                  case Data of
                    [] -> ok;
                    Data -> [Pid ! lists:last(Data) || Pid <- Pids]
                  end                 
                end,
                Clients),
  Delay = calc_delay(1000, T1),
  erlang:send_after(Delay, self(), tick),
  {noreply, State};



handle_info({'DOWN', _, _, Pid, _}, #subscriptions{clients=Clients0}=State) ->
  Clients = [{Query, lists:delete(Pid, Pids)} 
             || {Query, Pids} <- Clients0, 
             Pids =/= [Pid]],
  {noreply, State#subscriptions{clients=Clients}}.


calc_delay(Desired, T1) ->
  case Desired - timer:now_diff(erlang:now(), T1) div 1000 of
    D when D < 0 -> 1;
    D -> D
  end.