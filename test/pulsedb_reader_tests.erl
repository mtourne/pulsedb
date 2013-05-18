-module(pulsedb_reader_tests).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

% -import(stockdb_test_helper, [tempfile/1, tempdir/0, chunk_content/1]).


read_events_test() ->
  file:delete("read-test1.pulse"),
  {ok, P1} = pulsedb_appender:open("read-test1.pulse"),
  {ok, P2} = pulsedb_appender:append({row, 1368872568737, [45,23]}, P1),
  {ok, P3} = pulsedb_appender:append({row, 1368872568747, [40,27]}, P2),
  pulsedb_appender:close(P3),

  [{presense, Presense}] = pulsedb_reader:file_info("read-test1.pulse", [presense]),


  {ok, R1} = pulsedb_reader:open("read-test1.pulse"),

  {ok, I1} = pulsedb_iterator:init(R1),

  {{row,1368872568737, [45,23]}, I2} = pulsedb_iterator:read_event(I1),
  {{row,1368872568747, [40,27]}, I3} = pulsedb_iterator:read_event(I2),
  {eof,_} = pulsedb_iterator:read_event(I3),
  ok.





% file_info_test() ->
%   File = tempfile("write-append-test.temp"),
%   ok = filelib:ensure_dir(File),
%   file:delete(File),

%   stockdb_appender:write_events(File, chunk_content('109') ++ chunk_content('110_1'), 
%     [{stock, 'TEST'}, {date, {2012,7,25}}, {depth, 3}, {scale, 200}, {chunk_size, 300}]),

%   ?assertEqual([{date,{2012,7,25}},{scale,200},{depth,3}, {presence,{289,[109,110]}}],
%   	stockdb_reader:file_info(File, [date, scale, depth, presence])),
%   ?assertEqual([{candle,{12.33,12.45,12.23,12.445}}], stockdb_reader:file_info(File, [candle])),
%   file:delete(File).


% candle_test() ->
%   Root = stockdb:get_value(root),
%   ok = application:set_env(fix, root, tempdir()),
%   file:delete(stockdb_fs:path('TEST', "2012-07-25")),
%   stockdb:write_events('TEST', "2012-07-25", chunk_content('109') ++ chunk_content('110_1'), [{scale, 200}]),
%   Candle = stockdb:candle('TEST', "2012-07-25"),
%   application:set_env(fix, root, Root),
%   file:delete(stockdb_fs:path('TEST', "2012-07-25")),
%   ?assertEqual({12.33,12.45,12.23,12.445}, Candle),
%   ok.
