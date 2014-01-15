-module(pulsedb_SUITE).
-compile(export_all).



all() ->
  [{group, append_and_read}].


groups() ->
  [{append_and_read, [parallel], [
    append_and_read,
    worker_append_and_read,
    parse_query,
    % forbid_to_read_after_append,
    % forbid_to_append_after_read,
    % merge,
    info
  ]}].



init_per_suite(Config) ->
  application:set_env(pulsedb,ticks_per_hour,60000),
  Config.


end_per_suite(Config) ->
  Config.



append_and_read(_) ->
  {ok, DB1} = pulsedb:open("test/v3/pulse_rw"),

  Ticks1 = [
    {<<"input">>, 120, 5, [{name, <<"source1">>}]},
    {<<"output">>, 120, 0, [{name, <<"source1">>}]},

    {<<"input">>, 120, 2, [{name, <<"source2">>}]},
    {<<"output">>, 120, 2, [{name, <<"source2">>}]},


    {<<"input">>, 130, 10, [{name, <<"source1">>}]},
    {<<"output">>, 130, 2, [{name, <<"source1">>}]},

    {<<"input">>, 140, 3, [{name, <<"source1">>}]},
    {<<"output">>, 140, 6, [{name, <<"source1">>}]}
  ],
  {ok, DB2} = pulsedb:append(Ticks1, DB1),

  Ticks2 = [
    {<<"input">>, 4000121, 5, [{name, <<"source1">>}]},
    {<<"output">>, 4000121, 0, [{name, <<"source1">>}]},

    {<<"input">>, 4000122, 10, [{name, <<"source1">>}]},
    {<<"output">>, 4000122, 2, [{name, <<"source1">>}]},

    {<<"input">>, 4000122, 4, [{name, <<"source2">>}]},
    {<<"output">>, 4000122, 4, [{name, <<"source2">>}]},

    {<<"input">>, 4000123, 3, [{name, <<"source1">>}]},
    {<<"output">>, 4000123, 6, [{name, <<"source1">>}]}
  ],
  {ok, DB3} = pulsedb:append(Ticks2, DB2),

  pulsedb:close(DB3),

  {ok, ReadDB1} = pulsedb:open("test/v3/pulse_rw"),

  {ok, [
    {120,5},
    {130,10},
    {140,3},
    {4000121,5},
    {4000122,10},
    {4000123,3}
  ], ReadDB2} = pulsedb:read(<<"input">>, [{name,<<"source1">>}, {from, "1970-01-01"},{to,"1971-02-02"}], ReadDB1),


  {ok, [
    {120,5},
    {130,10},
    {140,3}
  ], ReadDB3} = pulsedb:read(<<"input">>, [{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], ReadDB2),

  {ok, [
    {120,7},
    {130,10},
    {140,3},
    {4000121,5},
    {4000122,14},
    {4000123,3}
  ], ReadDB4} = pulsedb:read(<<"input">>, [{from, "1970-01-01"},{to,"1971-01-02"}], ReadDB3),

  os:cmd("rm -rf test/v3/pulse_rw/1970/01/01"),

  % {ok, R1} = pulsedb:open("test/v2/pulse_rw"),
  % {ok, Ticks2, R2} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-04"}], R1),
  % pulsedb:close(R2).
  ok.



worker_append_and_read(_) ->
  {ok, DB1} = pulsedb:open(worker_ar, [{url, "file://test/v3/worker_rw"}]),

  Ticks1 = [
    {<<"input">>, 120, 5, [{name, <<"source1">>}]},
    {<<"input">>, 120, 2, [{name, <<"source2">>}]},
    {<<"input">>, 130, 10, [{name, <<"source1">>}]},
    {<<"input">>, 140, 3, [{name, <<"source1">>}]}
  ],
  pulsedb:append(Ticks1, DB1),

  Ticks2 = [
    {<<"input">>, 4000121, 5, [{name, <<"source1">>}]},
    {<<"input">>, 4000122, 10, [{name, <<"source1">>}]},
    {<<"input">>, 4000122, 4, [{name, <<"source2">>}]},
    {<<"input">>, 4000123, 3, [{name, <<"source1">>}]}
  ],
  pulsedb:append(Ticks2, DB1),

  {ok, [
    {120,5},
    {130,10},
    {140,3},
    {4000121,5},
    {4000122,10},
    {4000123,3}
  ], _} = pulsedb:read(<<"input">>, [{name,<<"source1">>}, {from, "1970-01-01"},{to,"1971-02-02"}], DB1),


  {ok, [
    {120,5},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"input">>, [{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], DB1),

  {ok, [
    {120,7},
    {130,10},
    {140,3},
    {4000121,5},
    {4000122,14},
    {4000123,3}
  ], _} = pulsedb:read(<<"input">>, [{from, "1970-01-01"},{to,"1971-01-02"}], DB1),


  {ok, [
    {120,5},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"sum:input{name=source1,from=1970-01-01,to=1970-01-02}">>, DB1),

  pulsedb:close(DB1),

  os:cmd("rm -rf test/v3/worker_rw/1970/01/01"),
  ok.


% forbid_to_read_after_append(_) ->
%   {ok, DB0} = pulsedb:open("test/v2/forbid_to_read"),

%   Ticks1 = [
%     #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
%     #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]},
%     #tick{name = <<"source1">>, utc = 23, value = [{input,3},{output,6}]}
%   ],
%   {ok, DB1} = pulsedb:append(Ticks1, DB0),

%   {error, _} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], DB1),
%   ok.


% forbid_to_append_after_read(_) ->
%   {ok, DB0} = pulsedb:open("test/v2/forbid_to_append"),

%   Ticks1 = [
%     #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
%     #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]},
%     #tick{name = <<"source1">>, utc = 23, value = [{input,3},{output,6}]}
%   ],
%   {ok, DB1} = pulsedb:append(Ticks1, DB0),
%   pulsedb:close(DB1),

%   {ok, DB2} = pulsedb:open("test/v2/forbid_to_append"),
%   {ok, _, DB3} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], DB2),
%   {error, _} = pulsedb:append(Ticks1, DB3),

%   ok.


info(_) ->
  {ok, DB0} = pulsedb:open("test/v2/info"),

  Ticks1 = [
    {<<"input">>,  21,  5, [{name, <<"source1">>}]},
    {<<"output">>, 21,  0, [{name, <<"source1">>}]},
   
    {<<"input">>,  22, 10, [{name, <<"source1">>}]},
    {<<"output">>, 22,  2, [{name, <<"source1">>}]},
   
    {<<"input">>,  23,  3, [{name, <<"source1">>}]},
    {<<"output">>, 23,  6, [{name, <<"source1">>}]}],
  {ok, DB1} = pulsedb:append(Ticks1, DB0),

  Ticks2 = [
    {<<"x">>, 21,  5, [{name, <<"source2">>}, {host, <<"t1">>}]},
    {<<"y">>, 21,  0, [{name, <<"source2">>}, {host, <<"t1">>}]},
   
    {<<"x">>, 22, 10, [{name, <<"source2">>}, {host, <<"t2">>}]},
    {<<"y">>, 22,  2, [{name, <<"source2">>}, {host, <<"t2">>}]},
   
    {<<"x">>, 23,  3, [{name, <<"source2">>}, {host, <<"t1">>}]},
    {<<"y">>, 23,  6, [{name, <<"source2">>}, {host, <<"t1">>}]}],
  {ok, DB2} = pulsedb:append(Ticks2, DB1),

  Info1 = pulsedb:info(DB2),
  {_,Metrics1} = lists:keyfind(sources,1,Info1),
  {_,Tags1} = lists:keyfind(<<"input">>,1,Metrics1),
  TagNames1 = [Name || {Name,_} <- Tags1],
  [<<"name">>] = TagNames1,

  pulsedb:close(DB2),

  Info2 = pulsedb:info("test/v2/info"),
  {_,Metrics2} = lists:keyfind(sources,1,Info2),
  {_,Tags2} = lists:keyfind(<<"x">>,1,Metrics2),
  TagNames2 = [Name || {Name,_} <- Tags2],
  [<<"host">>, <<"name">>] = lists:sort(TagNames2),
  ok.




parse_query(_) ->
  {<<"cpu">>, <<"max">>, []} = pulsedb:parse_query("max:cpu"),
  {<<"cpu">>, <<"max">>, [{<<"host">>,<<"flu1">>}]} = pulsedb:parse_query("max:cpu{host=flu1}"),
  {<<"media_output">>, <<"sum">>, [{<<"media">>,<<"ort">>},{<<"account">>,<<"1970-01-01">>}]} = 
    pulsedb:parse_query("sum:media_output{media=ort,account=1970-01-01}"),
  {<<"cpu">>, <<"max">>, [{<<"host">>,<<"flu1">>},{from,<<"123">>},{to,<<"456">>}]} = pulsedb:parse_query("max:cpu{host=flu1,from=123,to=456}"),

  {<<"cpu">>, <<"max">>, [{<<"host">>,<<"flu1">>},{from,<<"1970-01-01">>},{to,<<"456">>}]} = pulsedb:parse_query("max:cpu{host=flu1,from=1970-01-01,to=456}"),
  ok.


% merge(_) ->
%   {ok, DB0} = pulsedb:open("test/v2/merge"),

%   Ticks1 = [
%     #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
%     #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]},
%     #tick{name = <<"source1">>, utc = 23, value = [{input,3},{output,6}]}
%   ],
%   {ok, DB1} = pulsedb:append(Ticks1, DB0),

%   Ticks2 = [
%     #tick{name = <<"source1">>, utc = 11, value = [{input,5},{output,0}]},
%     #tick{name = <<"source1">>, utc = 12, value = [{input,10},{output,2}]},
%     #tick{name = <<"source1">>, utc = 13, value = [{input,3},{output,6}]},
%     #tick{name = <<"source1">>, utc = 21, value = [{input,5},{output,0}]},
%     #tick{name = <<"source1">>, utc = 22, value = [{input,10},{output,2}]}
%   ],
%   {ok, 3, DB2} = pulsedb:merge(Ticks2, DB1),
%   pulsedb:close(DB2),


%   Ticks3 = lists:sublist(Ticks2,1,3)++Ticks1,
%   {ok, DB3} = pulsedb:open("test/v2/merge"),
%   {ok, Ticks3, DB4} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], DB3),
%   pulsedb:close(DB4),

%   ok.

