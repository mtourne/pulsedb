-module(pulsedb_embed_resolver).
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/1, resolve/1]).
-export([init/1, terminate/2]).
-export([handle_call/3]).

-record(embed_resolver, {auth, resolve_url}).

-define(HTTP_REQUEST_TIMEOUT, 5000).
-define(TABLE, embed_resolver_cache).
-define(LEASE_TIME, 15*60).

start_link(Opts) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).



resolve(Embed) ->
  {Now,_} = pulsedb:current_second(),
  Query = ets:fun2ms(fun({Key,Value,Lease}) when Key == Embed andalso Lease > Now -> Value end),
  
  case ets:select(?TABLE, Query) of
    []      -> gen_server:call(?MODULE, {resolve, Embed});
    [Value] -> Value
  end.


init(Opts) ->
  Auth = lists:keyfind(auth, 1, Opts),
  ResolveUrl = lists:keyfind(embed_resolver, 1, Opts),
  State = #embed_resolver{auth = Auth, resolve_url = ResolveUrl},
  ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
  {ok, State}.


terminate(_,_) ->
  ok.


handle_call({resolve, Embed}, _From, #embed_resolver{auth=Auth, resolve_url=ResolveUrl}=State) ->
  Result = try resolve_embed(Embed, ResolveUrl) of
    {ok, Resolved} ->
      try decrypt_embed(Resolved, Auth)
      catch C:E -> {error, {C,E}}
      end;
    Other -> 
      Other
  catch
    C:E -> {error, {C,E}}
  end,

  {Now,_} = pulsedb:current_second(),
  Lease = Now + ?LEASE_TIME,
  ets:insert(?TABLE, {Embed, Result, Lease}),
  {reply, Result, State}.




resolve_embed(<<"full-", Embed/binary>>, _) ->
  {ok, Embed};

resolve_embed(ShortEmbed, {embed_resolver, BaseURL}) ->
  URL = iolist_to_binary([BaseURL, "/", ShortEmbed]),
  case lhttpc:request(URL, get, [], ?HTTP_REQUEST_TIMEOUT) of
    {ok,{{200,_},_,Embed}}  -> {ok, Embed};
    {ok,{{403,_},_,Reason}} -> {deny, Reason};
    {ok,{{404,_},_,Reason}} -> {not_found, Reason};
    {error, Reason}         -> {error, Reason};
    Other                   -> {error, Other}
  end.


decrypt_embed(Embed, {auth,AuthModule,AuthArgs}) ->
  AuthModule:decrypt(Embed, AuthArgs);

decrypt_embed(Embed, false) ->
  {ok, <<"Graphic">>, [Embed]}.