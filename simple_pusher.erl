#!/usr/bin/env escript
%%
%%! -env ERL_LIBS .. -pa ebin

-mode(compile).

main([URL|Args]) ->

  Auth = case Args of
    [] -> [];
    [Key, TagValues] when length(Key) == 16 ->
      Tags = [ list_to_tuple([list_to_binary(T1) || T1 <- string:tokens(T,"=")]) || T <- string:tokens(TagValues, ":")],
      ApiKey = pulsedb_netpush_auth:make_api_key(list_to_binary(Key), Tags),
      ["pulsedb-api-key: ", ApiKey, "\r\n"]
  end,
  {ok, {pulse, _, Host, Port, _, _}} = http_uri:parse(URL),
  {ok, Sock} = gen_tcp:connect(Host, Port, [binary,{active,false},{packet,http}]),

  Path = "/api/v1/pulse_push",
  ok = gen_tcp:send(Sock, ["CONNECT ", Path, " HTTP/1.1\r\n",
    "Host: ", Host, "\r\n",
    Auth,
    "Connection: Upgrade\r\n"
    "Upgrade: application/timeseries-text\r\n"
    "\r\n"]),
  {ok, {http_response, _, 101, _}} = gen_tcp:recv(Sock, 0),
  fetch_headers(Sock),
  inet:setopts(Sock, [{packet,line}]),

  U = 1390288134,
  gen_tcp:send(Sock, ["utc ", integer_to_list(U), "\n"]),
  [gen_tcp:send(Sock, io_lib:format("metric ~B media_output:media=ort~B:account=max@erlyvideo.org:point=cdbb536b-efa1-4802-a0e4-d8bcbdcd4ed9\n",[I,I])) ||
    I <- lists:seq(1,200)],
  T1 = erlang:now(),
  push(1, Sock),
  T2 = erlang:now(),
  io:format("Pushed 24 hours during ~B us\n", [timer:now_diff(T2,T1)]),
  ok.


push(86400, _Sock) ->
  ok;

push(N, Sock) ->
  gen_tcp:send(Sock, ["1 1 ", shift_value(random:uniform(10000) + 9000), "\n"]),

  [gen_tcp:send(Sock, [integer_to_list(I), " 0 ", shift_value(random:uniform(10000) + 9000), "\n"]) ||
    I <- lists:seq(2,200)],
  case N rem 100 of
    0 ->
      ok = gen_tcp:send(Sock, ["ping ", integer_to_list(N), "\n"]),
      Pong = iolist_to_binary(["pong ",integer_to_list(N),"\n"]),
      io:format("ping pong ~B\n", [N]),
      {ok, Pong} = gen_tcp:recv(Sock, 0);
    _ ->
      ok
  end,
  push(N+1,Sock).



shift_value(Value) when Value >= 0 andalso Value < 16#4000 -> integer_to_list(Value);
shift_value(Value) when Value >= 16#1000 andalso Value < 16#400000 -> integer_to_list(Value bsr 10)++"K";
shift_value(Value) when Value >= 16#1000000 andalso Value < 16#400000000 -> integer_to_list(Value bsr 20)++"M";
shift_value(Value) when Value >= 16#1000000000 andalso Value < 16#200000000000 -> integer_to_list(Value bsr 30)++"G";
shift_value(Value) when Value >= 16#1000000000000 andalso Value < 16#200000000000000 -> integer_to_list(Value bsr 40)++"T".


fetch_headers(Sock) ->
  case gen_tcp:recv(Sock, 0) of
    {ok, http_eoh} -> ok;
    {ok, {http_header, _, _, _, _}} -> fetch_headers(Sock)
  end.

