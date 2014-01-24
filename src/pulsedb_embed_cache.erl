-module(pulsedb_embed_cache).
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/0, read/1, write/2]).
-export([init/1, terminate/2]).
-export([handle_info/2]).

-record(embed_cache, {cleanup_timer}).

-define(CLEANUP_INTERVAL, 60*1000).
-define(TABLE, embed_cache).
-define(TABLE_LIMIT, 1000).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).



read(Embed) ->
  Query = ets:fun2ms(fun({Key,Value,_}) when Key == Embed -> Value end),
  case ets:select(?TABLE, Query) of
    []      -> undefined;
    [Value] -> {ok, Value}
  end.

write(Embed, Value) ->
  {Now,_} = pulsedb:current_second(),
  ets:insert(?TABLE, {Embed, Value, Now}),
  {ok, Value}.


init([]) ->
  ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
  Timer = erlang:send_after(?CLEANUP_INTERVAL, self(), cleanup),
  State = #embed_cache{cleanup_timer = Timer},
  {ok, State}.


terminate(_,_) ->
  ok.


handle_info(cleanup, #embed_cache{cleanup_timer=Timer0}=State) ->
  catch erlang:cancel_timer(Timer0),
  Info = ets:info(?TABLE),
  Size = proplists:get_value(size, Info),
  if Size > ?TABLE_LIMIT ->
      Sorted = lists:sort(ets:select(?TABLE, [{{'_','_','$3'},[],['$3']}])),
      UTC = lists:nth(Size - ?TABLE_LIMIT, Sorted),
      %% drops at least Size - ?TABLE_LIMIT old items
      ets:select_delete(fun({_,_,UTC0}) -> UTC0 < UTC end);
    true -> 
      ok
  end,
  Timer = erlang:send_after(?CLEANUP_INTERVAL, self(), cleanup),
  {noreply, State#embed_cache{cleanup_timer = Timer}}.

