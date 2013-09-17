#!/usr/bin/env escript

-mode(compile).

-include("include/pulsedb.hrl").


main([]) ->
  code:add_pathz("ebin"),
  os:cmd("rm -rf benchdb"),
  SourceCount = 100,
  MinuteCount = 1440,
  T1 = erlang:now(),
  {ok, DB1} = pulsedb:open("benchdb"),
  Sources = [ <<"source", (integer_to_binary(I))/binary>> || I <- lists:seq(1,SourceCount)],

  DB2 = write_minutes(Sources, MinuteCount, DB1),
  pulsedb:close(DB2),
  T2 = erlang:now(),

  Count = SourceCount*MinuteCount*60*2,
  Time = timer:now_diff(T2,T1),

  Size_ = os:cmd("du -k -d 0 benchdb"),
  {match, [Size__]} = re:run(Size_, "(\\d+)", [{capture,all_but_first,list}]),
  Size = list_to_integer(Size__) / 1024,
  RawSize = Count*8 / (1024*1024),
  Ratio = RawSize / Size,
  io:format("written ~B data points in ~B seconds: ~B us per point. on disk: ~.1f MB, raw: ~.1f MB, ratio: ~.1f \n", 
    [Count, Time div 1000000, Time div Count, Size, RawSize, Ratio]),

  T4 = erlang:now(),
  {ok, DB3} = pulsedb:open("benchdb"),
  {ok, Ticks, DB4} = pulsedb:read([{name,<<"source2">>},{from,"1970-01-01"},{to,"1970-01-02"}], DB3),
  pulsedb:close(DB4),
  T5 = erlang:now(),
  Time2 = timer:now_diff(T5,T4),
  io:format("Extracted ~B ticks during ~B ms, ~B us per tick\n", [length(Ticks), Time2 div 1000, Time2 div length(Ticks)]),
  os:cmd("rm -rf benchdb"),
  ok.

write_minutes(_Sources, 0, DB) ->
  DB;

write_minutes(Sources, Count, DB) ->
  if Count rem 100 == 0 -> io:format("write minute ~B\n", [Count]); true -> ok end,
  DB1 = write_minute(Sources, Count*60, DB),
  write_minutes(Sources, Count - 1, DB1).


write_minute([], _, DB) ->
  DB;

write_minute([Source|Sources], Base, DB) ->
  Ticks = [ #tick{name = Source, utc = Base+I, value = [
    {input,13423424324+random:uniform(4321432)},{output,134224324+random:uniform(4332)}
  ]} || I <- lists:seq(0,59)],
  {ok, DB1} = pulsedb:append(Ticks, DB),
  write_minute(Sources, Base, DB1).
