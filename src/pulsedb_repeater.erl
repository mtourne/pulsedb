-module(pulsedb_repeater).

-export([start_link/0]).
-export([init/1, terminate/2]).



start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
  {ok, state}.



terminate(_,_) ->
  ok.

