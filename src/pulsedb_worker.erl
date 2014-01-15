-module(pulsedb_worker).
-export([start_link/2]).
-export([append/2, read/3]).

-export([init/1, handle_call/3, terminate/2]).



start_link(Name, Options) ->
  gen_server:start_link({local, Name}, ?MODULE, [Options], []).


append(Ticks, DB) when is_pid(DB) orelse is_atom(DB) ->
  gen_server:call(DB, {append, Ticks}).


read(Name, Query, DB) ->
  gen_server:call(DB, {read, Name, Query}).

-record(worker, {
  db
}).

init([Options]) ->
  {ok, DB} = pulsedb:open(undefined, Options),
  {ok, #worker{db = DB}}.


handle_call({append, Ticks}, _, #worker{db = DB} = W) ->
  {ok, DB1} = pulsedb:append(Ticks, DB),
  {reply, ok, W#worker{db = DB1}};

handle_call({read, Name, Query}, _, #worker{db = DB} = W) ->
  {ok, Ticks, DB2} = pulsedb:read(Name, Query, DB),
  {reply, {ok, Ticks, self()}, W#worker{db = DB2}}.


terminate(_,_) ->
  ok.

