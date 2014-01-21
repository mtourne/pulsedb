-module(pulsedb_SUITE).
-compile(export_all).



all() ->
  [{group, append_and_read}].


groups() ->
  [{append_and_read, [parallel], [
    append_and_read,
    worker_append_and_read,
    netpush_403,
    netpush_append,
    netpush_ssl_append,
    worker_cleanup,
    parse_query,
    collector,
    collector_with_backend,
    collector_with_dead_backend,
    collector_to_minute,
    % autohealing,
    downsampling,
    replicator,
    % forbid_to_read_after_append,
    % forbid_to_append_after_read,
    info,
    required_dates,
    query_mutation
  ]}].



key() -> <<"0123456789abcdef">>.

init_per_suite(Config) ->
  Apps = [pulsedb,crypto,asn1,public_key,ssl,ranch,cowlib,cowboy],
  [{ok,App} = {application:start(App),App} || App <- Apps],

  R = [{apps,Apps}],
  {ok, Pid} = pulsedb:open(netpush_db, [{url, "file://test/v3/netpush_db"}]),
  unlink(Pid),


  Port = 6801,
  NetAuth = [{key,key()}],
  Dispatch = [{'_', [
    {"/api/v1/pulse_push", pulsedb_netpush_handler, [{db,netpush_db},{auth,pulsedb_netpush_auth,NetAuth}]}
  ]}],
  {ok, L} = ranch:start_listener(fake_pulsedb, 1, ranch_tcp, [{port,Port}], cowboy_protocol, [{env, [
    {dispatch, cowboy_router:compile(Dispatch)}
  ]}]),

  SslTransOpts = 
    [{certfile,find("flussonic.crt")},
      {keyfile,find("flussonic.key")},
      {password,"flussonic"},
      {reuse_sessions,true},
      {verify,verify_none},
      {ciphers, ciphers()}],

  {ok, L2} = ranch:start_listener(fake_pulsedb_ssl, 1, ranch_ssl, [{port,Port+1}|SslTransOpts], cowboy_protocol, [{env, [
    {dispatch, cowboy_router:compile(Dispatch)}
  ]}]),

  [{r,R}|Config].


find(Cert) ->
  filename:join(code:lib_dir(pulsedb,test), Cert).


ciphers() ->
  [S || S={Key,_,_} <- ssl:cipher_suites(), 
        binary:match(atom_to_binary(Key,utf8),<<"ec">>) =/= {0,2}].


end_per_suite(Config) ->
  erlang:exit(whereis(netpush_db),shutdown),
  R = proplists:get_value(r,Config),
  [application:stop(App) || App <- lists:reverse(proplists:get_value(apps,R))],
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

  % {ok, R1} = pulsedb:open("test/v2/pulse_rw"),
  % {ok, Ticks2, R2} = pulsedb:read([{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-04"}], R1),
  % pulsedb:close(R2).
  ok.



worker_append_and_read(_) ->
  {ok, DB1} = pulsedb:open(worker_ar, [{url, "file://test/v3/worker_rw"}]),

  Ticks1 = [
    {<<"input">>, 120, 6, [{name, <<"source1">>}]},
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
    {120,6},
    {130,10},
    {140,3},
    {4000121,5},
    {4000122,10},
    {4000123,3}
  ], _} = pulsedb:read(<<"input">>, [{name,<<"source1">>}, {from, "1970-01-01"},{to,"1971-02-02"}], DB1),


  {ok, [
    {120,6},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"input">>, [{name,<<"source1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], DB1),

  {ok, [
    {120,8},
    {130,10},
    {140,3},
    {4000121,5},
    {4000122,14},
    {4000123,3}
  ], _} = pulsedb:read(<<"input">>, [{from, "1970-01-01"},{to,"1971-01-02"}], DB1),


  {ok, [
    {120,6},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"sum:input{name=source1,from=1970-01-01,to=1970-01-02}">>, DB1),

  {ok, [
    {120,8},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"sum:input{from=1970-01-01,to=1970-01-02}">>, DB1),

  {ok, [
    {120,6},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"max:input{from=1970-01-01,to=1970-01-02}">>, DB1),

  {ok, [
    {120,4},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"avg:input{from=1970-01-01,to=1970-01-02}">>, DB1),

  {ok, [
  ], _} = pulsedb:read(<<"avg:input">>, DB1),



  pulsedb:close(DB1),

  ok.



netpush_403(_) ->
  {error, denied} = pulsedb:open(netpush_403_client, [{url, "pulse://localhost:6801/"}]),
  ok.


netpush_append(_) ->
  Apikey = pulsedb_netpush_auth:make_api_key(key(), [{<<"point">>, <<"p1">>}]),
  {ok, DB1} = pulsedb:open(netpush_client, [{url, "pulse://localhost:6801/"},{api_key,Apikey}]),

  Ticks1 = [
    {<<"input">>, 120, 6, [{name, <<"source-net1">>}]},
    {<<"input">>, 120, 2, [{name, <<"source-net2">>}]},
    {<<"input">>, 130, 10, [{name, <<"source-net1">>}]},
    {<<"input">>, 140, 3, [{name, <<"source-net1">>}]}
  ],
  pulsedb:append(Ticks1, netpush_client),

  Ticks2 = [
    {<<"input">>, 4000121, 5, [{name, <<"source-net1">>}]},
    {<<"input">>, 4000122, 10, [{name, <<"source-net1">>}]},
    {<<"input">>, 4000122, 4, [{name, <<"source-net2">>}]},
    {<<"input">>, 4000123, 3, [{name, <<"source-net1">>}]}
  ],
  pulsedb:append(Ticks2, netpush_client),
  pulsedb:sync(netpush_client),

  {ok, [
    {120,6},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"sum:input{name=source-net1,from=1970-01-01,to=1970-01-02}">>, netpush_db),


  % {ok, [
  %   {120,6},
  %   {130,10},
  %   {140,3},
  %   {4000121,5},
  %   {4000122,10},
  %   {4000123,3}
  % ], _} = pulsedb:read(<<"input">>, [{name,<<"source-net1">>}, {from, "1970-01-01"},{to,"1971-02-02"}], netpush_client),


  % {ok, [
  %   {120,6},
  %   {130,10},
  %   {140,3}
  % ], _} = pulsedb:read(<<"input">>, [{name,<<"source-net1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], netpush_client),

  % {ok, [
  %   {120,6},
  %   {130,10},
  %   {140,3}
  % ], _} = pulsedb:read(<<"sum:input{name=source-net1,from=1970-01-01,to=1970-01-02}">>, netpush_client),

  pulsedb:close(netpush_client),

  ok.








netpush_ssl_append(_) ->
  Apikey = pulsedb_netpush_auth:make_api_key(key(), [{<<"point">>, <<"p1">>}]),
  {ok, DB1} = pulsedb:open(netpush_ssl_client, [{url, "pulses://localhost:6802/"},{api_key,Apikey}]),

  Ticks1 = [
    {<<"input">>, 120, 6, [{name, <<"source-ssl1">>}]},
    {<<"input">>, 120, 2, [{name, <<"source-ssl2">>}]},
    {<<"input">>, 130, 10, [{name, <<"source-ssl1">>}]},
    {<<"input">>, 140, 3, [{name, <<"source-ssl1">>}]}
  ],
  pulsedb:append(Ticks1, netpush_ssl_client),

  Ticks2 = [
    {<<"input">>, 4000121, 5, [{name, <<"source-ssl1">>}]},
    {<<"input">>, 4000122, 10, [{name, <<"source-ssl1">>}]},
    {<<"input">>, 4000122, 4, [{name, <<"source-ssl2">>}]},
    {<<"input">>, 4000123, 3, [{name, <<"source-ssl1">>}]}
  ],
  pulsedb:append(Ticks2, netpush_ssl_client),
  pulsedb:sync(netpush_ssl_client),


  {ok, [
    {120,6},
    {130,10},
    {140,3}
  ], _} = pulsedb:read(<<"sum:input{name=source-ssl1,from=1970-01-01,to=1970-01-02}">>, netpush_db),


  % {ok, [
  %   {120,6},
  %   {130,10},
  %   {140,3},
  %   {4000121,5},
  %   {4000122,10},
  %   {4000123,3}
  % ], _} = pulsedb:read(<<"input">>, [{name,<<"source-ssl1">>}, {from, "1970-01-01"},{to,"1971-02-02"}], netpush_ssl_client),


  % {ok, [
  %   {120,6},
  %   {130,10},
  %   {140,3}
  % ], _} = pulsedb:read(<<"input">>, [{name,<<"source-ssl1">>}, {from, "1970-01-01"},{to,"1970-01-02"}], netpush_ssl_client),


  % {ok, [
  %   {120,6},
  %   {130,10},
  %   {140,3}
  % ], _} = pulsedb:read(<<"sum:input{name=source-ssl1,from=1970-01-01,to=1970-01-02}">>, netpush_ssl_client),



  pulsedb:close(netpush_ssl_client),

  ok.





worker_cleanup(_) ->
  {ok, DB1} = pulsedb:open(worker_cleanup, [{url, "file://test/v3/worker_cleanup"},{delete_older,86400}]),

  Ticks1 = [
    {<<"input">>, 120, 5, [{name, <<"source1">>}]},
    {<<"output">>, 120, 0, [{name, <<"source1">>}]}  
  ],
  pulsedb:append(Ticks1, worker_cleanup),

  {ok, [{_,5}], _} = pulsedb:read("sum:input{from=0,to=1000}", worker_cleanup),

  DB1 ! clean,
  sys:get_state(DB1),

  {ok, [], _} = pulsedb:read("sum:input{from=0,to=1000}", worker_cleanup),
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
  {ok, DB0} = pulsedb:open(test_info_db, <<"test/v2/info">>),

  Ticks1 = [
    {<<"input">>,  21,  5, [{name, <<"source1">>}]},
    {<<"output">>, 21,  0, [{name, <<"source1">>}]},
   
    {<<"input">>,  22, 10, [{name, <<"source1">>}]},
    {<<"output">>, 22,  2, [{name, <<"source1">>}]},
   
    {<<"input">>,  23,  3, [{name, <<"source1">>}]},
    {<<"output">>, 23,  6, [{name, <<"source1">>}]}
  ],

  pulsedb:append(Ticks1, test_info_db),

  Ticks2 = [
    {<<"x">>, 21,  5, [{name, <<"source2">>}, {host, <<"t1">>}]},
    {<<"y">>, 21,  0, [{name, <<"source2">>}, {host, <<"t1">>}]},
   
    {<<"x">>, 22, 10, [{name, <<"source2">>}, {host, <<"t2">>}]},
    {<<"y">>, 22,  2, [{name, <<"source2">>}, {host, <<"t2">>}]},
   
    {<<"x">>, 23,  3, [{name, <<"source2">>}, {host, <<"t1">>}]},
    {<<"y">>, 23,  6, [{name, <<"source2">>}, {host, <<"t1">>}]}
  ],

  pulsedb:append(Ticks2, test_info_db),
  pulsedb:sync(test_info_db),

  {ok, DB1} = pulsedb:open(<<"test/v2/info">>),

  Info1 = pulsedb:info(test_info_db),
  Info1_ = pulsedb:info(DB1),
  case Info1 of
    Info1_ -> ok;
    _ -> ct:pal("~p = ~p", [Info1, Info1_])
  end,
  Info1 = Info1_,
  {_,Metrics1} = lists:keyfind(sources,1,Info1),
  {_,Tags1} = lists:keyfind(<<"input">>,1,Metrics1),
  TagNames1 = [Name || {Name,_} <- Tags1],
  [<<"name">>] = TagNames1,

  pulsedb:close(DB1),

  Info2 = pulsedb:info(<<"test/v2/info">>),
  {_,Metrics2} = lists:keyfind(sources,1,Info2),
  {_,Tags2} = lists:keyfind(<<"x">>,1,Metrics2),
  TagNames2 = [Name || {Name,_} <- Tags2],
  [<<"host">>, <<"name">>] = lists:sort(TagNames2),
  ok.




parse_query(_) ->
  {undefined, undefined, <<"cpu">>, []} = pulsedb:parse_query("cpu"),
  {<<"max">>, undefined, <<"cpu">>, []} = pulsedb:parse_query("max:cpu"),
  {<<"max">>, undefined, <<"cpu">>, []} = pulsedb:parse_query("max:cpu{}"),
  {undefined, undefined, <<"cpu">>, []} = pulsedb:parse_query("cpu{}"),
  {<<"max">>, {6,<<"avg">>}, <<"cpu">>, []} = pulsedb:parse_query("max:6s-avg:cpu"),
  {<<"max">>, {600,<<"avg">>}, <<"cpu">>, []} = pulsedb:parse_query("max:10m-avg:cpu"),
  {<<"max">>, {600,<<"avg">>}, <<"cpu">>, []} = pulsedb:parse_query("max:10m-avg:cpu{}"),
  {<<"max">>, undefined, <<"cpu">>, [{<<"host">>,<<"flu1">>}]} = pulsedb:parse_query("max:cpu{host=flu1}"),
  {undefined, undefined, <<"cpu">>, [{<<"host">>,<<"flu1">>}]} = pulsedb:parse_query("cpu{host=flu1}"),
  {<<"sum">>, undefined, <<"media_output">>, [{<<"media">>,<<"ort">>},{<<"account">>,<<"1970-01-01">>}]} = 
    pulsedb:parse_query("sum:media_output{media=ort,account=1970-01-01}"),
  {<<"max">>, undefined, <<"cpu">>, [{<<"host">>,<<"flu1">>},{from,<<"123">>},{to,<<"456">>}]} = pulsedb:parse_query("max:cpu{host=flu1,from=123,to=456}"),

  {<<"max">>, undefined, <<"cpu">>, [{<<"host">>,<<"flu1">>},{from,<<"1970-01-01">>},{to,<<"456">>}]} = pulsedb:parse_query("max:cpu{host=flu1,from=1970-01-01,to=456}"),
  ok.



pulse_init(State) -> {ok, State}.
pulse_collect({Name,Val} = State) -> {reply, [{Name, Val, [{tag1,<<"value1">>}]}], State}.


collector(_) ->
  {ok, [], _} = pulsedb:read(<<"max:test1">>, seconds),
  {ok, Pid} = pulsedb:collect(<<"test_collector">>, ?MODULE, {test1,20}),
  Pid ! collect,
  sys:get_state(Pid),
  {ok, [{_,20}], _} = pulsedb:read(<<"max:test1">>, seconds),
  {ok, [{_,20}], _} = pulsedb:read(<<"max:test1{tag1=value1}">>, seconds),

  {ok, [{_,20}], _} = pulsedb:read(<<"max:test1">>, memory),
  {ok, [{_,20}], _} = pulsedb:read(<<"max:test1{tag1=value1}">>, memory),

  ok = pulsedb:stop_collector(<<"test_collector">>),
  {ok, [], _} = pulsedb:read(<<"max:test1">>, seconds),
  ok.



collector_with_backend(_) ->
  {ok, Pid0} = pulsedb:open(test_pulse_saver1, [{url, "file://test/test_pulse_saver1"}]),

  {Now,_} = pulsedb:current_second(),
  N1 = integer_to_list(Now - 60),
  N2 = integer_to_list(Now + 60),
  Range = "from="++N1++",to="++N2,

  {ok, [], _} = pulsedb:read(<<"max:test2">>, seconds),
  {ok, [], _} = pulsedb:read("max:test2{"++Range++"}", test_pulse_saver1),
  {ok, Pid} = pulsedb:collect(<<"test_collector2">>, ?MODULE, {test2,40}, [{copy, test_pulse_saver1}]),
  Pid ! collect,
  sys:get_state(Pid),
  {ok, [{_,40}], _} = pulsedb:read(<<"max:test2">>, seconds),
  {ok, [{_,40}], _} = pulsedb:read(<<"max:test2{tag1=value1}">>, seconds),

  {ok, [{_,40}], _} = pulsedb:read("sum:test2{"++Range++"}", test_pulse_saver1),
  {ok, [{_,40}], _} = pulsedb:read("sum:test2{tag1=value1,"++Range++"}", test_pulse_saver1),
  ok.


collector_to_minute(_) ->
  {Now,_} = pulsedb:current_second(),
  Minute = (Now div 60)*60,

  pulsedb:append([{<<"mmm">>, Minute - 10, 2, [{name, <<"src1">>}]}], seconds),
  pulsedb:append([{<<"mmm">>, Minute - 8, 10, [{name, <<"src1">>}]}], seconds),
  {ok, [{_,2},{_,10}], _} = pulsedb:read("mmm{from="++integer_to_list(Minute-240)++",to="++integer_to_list(Minute+40)++"}", seconds),
  {ok, [{_,2},{_,10}], _} = pulsedb:read("mmm{from="++integer_to_list(Minute-240)++",to="++integer_to_list(Minute+40)++"}", memory),

  pulsedb_memory:merge_seconds_data([{<<"mmm">>,[{<<"name">>,<<"src1">>}]}], Minute),
  {ok, [{_,6}], _} = pulsedb:read("mmm{from="++integer_to_list(Minute-240)++",to="++integer_to_list(Minute+40)++"}", minutes),
  {ok, [{_,6}], _} = pulsedb:read("sum:1m-avg:mmm{from="++integer_to_list(Minute-240)++",to="++integer_to_list(Minute+40)++"}", memory),

  ok.




collector_with_dead_backend(_) ->
  {Now,_} = pulsedb:current_second(),
  N1 = integer_to_list(Now - 60),
  N2 = integer_to_list(Now + 60),
  Range = "from="++N1++",to="++N2,

  {ok, [], _} = pulsedb:read(<<"max:test3">>, seconds),
  {ok, Pid} = pulsedb:collect(<<"test_collector3">>, ?MODULE, {test3,40}, [{copy, test_pulse_saver3}]),
  Pid ! collect,
  sys:get_state(Pid),
  {ok, [{_,40}], _} = pulsedb:read(<<"max:test3">>, seconds),
  {ok, [{_,40}], _} = pulsedb:read(<<"max:test3{tag1=value1}">>, seconds),

  ok.



autohealing(_) ->
  {ok, DB1} = pulsedb:open("test/v3/autohealing"),

  Ticks1 = [
    {<<"input">>, 120, 5, [{name, <<"source1">>}]},
    {<<"output">>, 120, 0, [{name, <<"source1">>}]}  
  ],
  {ok, DB2} = pulsedb:append(Ticks1, DB1),
  pulsedb:close(DB2),

  {ok, [{_,5}], _} = pulsedb:read(<<"max:input{from=0,to=200}">>, DB2),


  {ok, F} = file:open("test/v3/autohealing/1970/01/01/config_v3", [binary,write,raw]),
  file:pwrite(F, 3, <<"zzzzzz">>),
  file:close(F),


  {ok, DB3} = pulsedb:open("test/v3/autohealing"),
  catch pulsedb:append([{<<"input">>, 122, 4, [{name, <<"source1">>}]}], DB3),
  {ok, DB4} = pulsedb:append([{<<"input">>, 123, 3, [{name, <<"source1">>}]}], DB3),

  {ok, [{123,3}], _} = pulsedb:read(<<"max:input{from=0,to=200}">>, DB4),

  ok.


downsampling(_) ->
  {ok, _} = pulsedb:open(test_downsampling, <<"test/v3/downsampling">>),

  pulsedb:append([{<<"ds">>, 10, 4, []}], test_downsampling),
  pulsedb:append([{<<"ds">>, 12, 6, []}], test_downsampling),

  pulsedb:append([{<<"ds">>, 100, 4, []}], test_downsampling),
  pulsedb:append([{<<"ds">>, 102, 8, []}], test_downsampling),
  pulsedb:append([{<<"ds">>, 104, 23, []}], test_downsampling),

  pulsedb:append([{<<"ds">>, 724, 20, []}], test_downsampling),
  pulsedb:append([{<<"ds">>, 725, 24, []}], test_downsampling),

  {ok, [{0,9},{600,22}], _} = pulsedb:read("sum:10m-avg:ds{from=0,to=1800}", test_downsampling),
  {ok, [{0,45},{600,44}], _} = pulsedb:read("sum:10m-sum:ds{from=0,to=1800}", test_downsampling),
  {ok, [{0,23},{600,24}], _} = pulsedb:read("sum:10m-max:ds{from=0,to=1800}", test_downsampling),
  ok.



replicator(_) ->
  Self = self(),
  pulsedb:replicate(seconds),
  List = ets:tab2list(pulsedb_replicators),
  {seconds,_} = lists:keyfind(self(),2,List),
  % Pid = spawn_link(fun() ->
  %   pulsedb:replicate(seconds),
  %   receive
  %     M -> Self ! {replicated, M}
  %   end
  % end),

  pulsedb:append([{<<"repl">>, 10, 4, []}], seconds),
  pulsedb:append([{<<"repl">>, 12, 4, []}], seconds),

  receive
    {pulse, _, _, _, _, _} -> ok
  after
    500 -> 
      ct:pal("msg: ~p ~p", [self(), process_info(self(),messages)]),
      error(replication_not_working)
  end,

  ok.

required_dates(_) ->
  {ok, DB0} = pulsedb:open(required_dates_db, <<"test/v3/required_dates">>),

  TicksYesterday = [
    {<<"input">>, 1390224618,  1, [{name, <<"source1">>}]},
    {<<"input">>, 1390224619,  2, [{name, <<"source1">>}]}],
  pulsedb:append(TicksYesterday, required_dates_db),

  TicksToday = [
    {<<"input">>, 1390292577,  3, [{name, <<"source1">>}]},
    {<<"input">>, 1390292578,  4, [{name, <<"source1">>}]}],
  pulsedb:append(TicksToday, required_dates_db),
  
  
  {ok, [{1390224618,  1},
        {1390224619,  2},
        {1390292577,  3},
        {1390292578,  4}], _} = pulsedb:read(<<"input{from=1390224618,to=1390292579}">>, required_dates_db).



query_mutation(_) ->
  Q1s = <<"sum:10s-avg:ds{from=0,to=1800}">>,
  Q1 = pulsedb_query:parse(Q1s),
  
  
  Q1s = pulsedb_query:render(Q1),
  
  Q2 = pulsedb_query:add_tag({account, "test@email.com"}, Q1),
  <<"sum:10s-avg:ds{from=0,to=1800,account=test@email.com}">> = pulsedb_query:render(Q2),
  
  Q3 = pulsedb_query:remove_tag(from, Q1),
  <<"sum:10s-avg:ds{to=1800}">> = pulsedb_query:render(Q3),
  
  Q4 = pulsedb_query:remove_tag([from, to], Q1),
  <<"sum:10s-avg:ds">> = pulsedb_query:render(Q4),
  
  Q5_ = pulsedb_query:remove_tag([from, to], Q1),
  Q5 = pulsedb_query:add_tag([{account, "test@email.com"}, {from, 123}], Q5_),
  <<"sum:10s-avg:ds{account=test@email.com,from=123}">> = pulsedb_query:render(Q5).











