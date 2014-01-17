-module(pulsedb_realtime).
-export([subscribe/2, unsubscribe/1]).
-export([start_link/0, init/1, terminate/2, handle_info/2, handle_call/2]).

-record(subscriptions, 
 {
  clients=[] % query_key, {Name,Query}, pids
  }).

subscribe(Query, Tag) ->
  gen_server:call(?MODULE, {subscribe, self(), Query, Tag}).


unsubscribe(Tag) ->
  gen_server:call(?MODULE, {unsubscribe, self(), Tag}).


start_link() -> 
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
  erlang:send_after(0, self(), tick),
  {ok, #subscriptions{}}.


terminate(_,_) ->
  ok.


handle_call({subscribe, Pid, Query, Tag}, #subscriptions{clients=Clients0}=State) ->
  Monitor = erlang:monitor(process, Pid),
  Clients = case lists:keytake(Query, 1, Clients0) of
    {value, {Q, UTC, Pids0}, Rest} ->
      Pids = lists:usort([{Pid,Tag,Monitor}|Pids0]),
      [{Q,UTC, Pids} | Rest];
    false ->
      [{Query,undefined,[{Pid,Tag,Monitor}]} | Clients0]
  end,
  
  {noreply, State#subscriptions{clients=Clients}};


handle_call({unsubscribe, Pid, Tag}, #subscriptions{clients=Clients0}=State) ->
  Clients1 = [begin
                {Demonitor, Rest} = lists:partition(fun ({Pid0,Tag0,_}) ->
                                                      Pid0 == Pid andalso Tag0 == Tag
                                                    end, Pids),
                [erlang:demonitor(Ref) || {_,_,Ref} <- Demonitor],
                {Query, UTC, Rest}
                end
              || {Query,UTC,Pids} <- Clients0],
  Clients = [C || {Pids,_,_}=C <- Clients1, length(Pids) > 0],
  {noreply, State#subscriptions{clients=Clients}}.


handle_info(tick, #subscriptions{clients=Clients}=State) ->
  Clients1 = lists:map(fun({Query,LastUTC,Pids} = Entry) ->
    case pulsedb:read(Query, memory) of
      {ok, [], _} -> 
        Entry;
      {ok, Data, _} ->
        {UTC, V} = lists:last(Data),
        if
          LastUTC == undefined orelse UTC > LastUTC ->
            [Pid ! {pulse, Tag, UTC, V} || {Pid,Tag,_} <- Pids],
            {Query, UTC, Pids};
          true ->
            Entry
        end                 
    end
  end, Clients),
  {_,Delay} = pulse:current_second(),
  erlang:send_after(Delay, self(), tick),
  {noreply, State#subscriptions{clients = Clients1}};



handle_info({'DOWN', _, _, Pid, _}, #subscriptions{clients=Clients0}=State) ->
  Clients1 = [{Query, UTC, lists:keydelete(Pid,1,Pids)} 
             || {Query, UTC, Pids} <- Clients0],
  Clients = [C || {Pids,_,_}=C <- Clients1, length(Pids) > 0],
  {noreply, State#subscriptions{clients=Clients}}.
