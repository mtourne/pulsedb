-module(pulsedb_manager).
-export([start_link/1, append/2, read/3, info/1]).
-export([init/1, terminate/2, handle_cast/2, handle_call/3]).

start_link(Root) -> 
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Root], []).

append(DbName, Tick) ->
  gen_server:cast(?MODULE, {append, DbName, Tick}).

read(DbName, Name, Query) ->
  gen_server:call(?MODULE, {read, DbName, Name, Query}).

info(DbName) ->
  gen_server:call(?MODULE, {info, DbName}).
  

-record(db_manager, 
 {
  %{name, {db, path}}
  instances = [],
  root
  }).

init([Root]) ->
  {ok, #db_manager{root = Root}}.


terminate(_,_) ->
  ok.


handle_cast({append, DbName, Tick}, #db_manager{instances=Instances, root=Root} = State) ->
  case proplists:get_value(DbName, Instances) of
    undefined -> 
      {ok, DB0} = pulsedb:open(db_path(Root, DbName)),
      {ok, DB1} = pulsedb:append(Tick, DB0),
      State1 = State#db_manager{instances=[{DbName, DB1}|Instances]},
      {noreply, State1};
    DB0 ->
      {ok, DB1} = pulsedb:append(Tick, DB0),
      State1 = State#db_manager{instances=lists:keystore(DbName, 1, Instances, {DbName, DB1})},
      {noreply, State1}
  end.


handle_call({read, DbName, Name, Query}, _From, #db_manager{root=Root} = State) ->
  {ok, DB0} = pulsedb:open(db_path(Root, DbName)),
  {ok, Ticks, DB1} = pulsedb:read(Name, Query, DB0),
  {reply, Ticks, State};

handle_call({info, DbName}, _From, #db_manager{root=Root} = State) ->
  Info = pulsedb:info(db_path(Root, DbName)),
  {reply, Info, State}.


db_path(Root, [DbName, Resolution]) ->
  iolist_to_binary([Root, "/", DbName, "/", atom_to_list(Resolution)]);

db_path(Root, DbName) ->
  iolist_to_binary([Root, "/", DbName]).















