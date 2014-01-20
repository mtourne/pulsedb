-module(pulsedb_realtime).
-export([subscribe/2, unsubscribe/1, clean_query/1]).
-export([start_link/0, init/1, terminate/2, handle_info/2, handle_call/3]).

-record(subscriptions, 
 {
  clients=[] % query_key, {Name,Query}, pids
  }).

subscribe(Query0, Tag) ->
  Query = clean_query(Query0),
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


handle_call({subscribe, Pid, Query, Tag},_, #subscriptions{clients=Clients0}=State) ->
  Clients = case lists:keytake(Query, 1, Clients0) of
    {value, {Q, UTC, Pids0}, Rest} ->
      Pids = case find_subscription(Pid, Tag, Pids0) of
        {[], Pids0} -> 
          Ref = erlang:monitor(process, Pid),
          [{Pid, Tag, Ref}|Pids0];
        _ -> 
          Pids0
      end,
      [{Q,UTC,Pids} | Rest];
    false ->
      Ref = erlang:monitor(process, Pid),
      [{Query,undefined,[{Pid,Tag,Ref}]} | Clients0]
  end,
  
  {reply, ok, State#subscriptions{clients=Clients}};


handle_call({unsubscribe, Pid, Tag},_, #subscriptions{clients=Clients0}=State) ->
  Clients1 = [begin
                {Demonitor, Rest} = find_subscription(Pid, Tag, Pids),
                [erlang:demonitor(Ref) || {_,_,Ref} <- Demonitor],
                {Query, UTC, Rest}
                end
              || {Query,UTC,Pids} <- Clients0],
  Clients = [C || {Pids,_,_}=C <- Clients1, length(Pids) > 0],
  {reply, ok, State#subscriptions{clients=Clients}}.


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
  {_,Delay} = pulsedb:current_second(),
  erlang:send_after(Delay, self(), tick),
  {noreply, State#subscriptions{clients = Clients1}};



handle_info({'DOWN', _, _, Pid, _}, #subscriptions{clients=Clients0}=State) ->
  Clients1 = [{Query, UTC, lists:keydelete(Pid,1,Pids)} 
             || {Query, UTC, Pids} <- Clients0],
  Clients = [C || {Pids,_,_}=C <- Clients1, length(Pids) > 0],
  {noreply, State#subscriptions{clients=Clients}}.


find_subscription(Pid, Tag, Pids) ->
  lists:partition(fun ({Pid0,Tag0,_}) ->
                    Pid0 == Pid andalso Tag0 == Tag
                  end, Pids).

clean_query(Query0) ->
  {Aggregator, Downsampler, Name, Tags0} = pulsedb:parse_query(Query0),
  Tags = [{K,V} || {K,V} <- Tags0, K =/= from andalso K =/= to],
  %agg:aggregator ":" ds:downsampler ":" mn:metric_name "{" tags:tags / 
  Parts = [
    case Aggregator of 
      undefined -> <<>>;
      _ -> <<Aggregator/binary, ":">>
    end,
    case Downsampler of
      {N_,Fn} -> 
        N = integer_to_binary(N_),
        <<N/binary,"s-",Fn/binary,":">>;
      _ -> <<>>
    end,
    Name,
    tags_to_text(lists:usort(Tags))],
  iolist_to_binary(Parts).

tags_to_text([]) -> <<>>;
tags_to_text(Tags_) -> 
  Tags = tags_to_text0(Tags_),
  <<"{", Tags/binary, "}">>.

tags_to_text0([Tag0]) -> tag_to_text(Tag0);
tags_to_text0([Tag0|Rest0]) ->
  Tag = tag_to_text(Tag0),
  Rest = tags_to_text0(Rest0),
  <<Tag/binary, ",", Rest/binary>>.

tag_to_text({Tag, Value}) when is_atom(Tag) -> 
  tag_to_text({atom_to_binary(Tag, latin1), Value});
tag_to_text({Tag, Value}) when is_binary(Tag) ->
  <<Tag/binary, "=", Value/binary>>.

