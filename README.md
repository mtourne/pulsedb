Pulsedb
=======


This is library and a server a storage for time series. You can use it for storing metrics like network usage, load, etc..

You can use it as an embedded library for Erlang servers or use it as a standalone HTTP server.


Data model
----------

All data is organized in metrics. Metric is a composition of name and tags:

    out_speed interface=eth0,host=host1.myservice.com

Each measured data point is identified by metric and UTC of this measurement.

Pulsedb writes data to files sharded by date. Cleanup of old data is done by deleting files with old date.

When you read data from pulsedb, it can aggregate metrics with equal names according to tag query.

For example, if you have several datacenters with many racks inside, you can write following metrics:

    out_speed 1378950600 4324141232 host=host1.myservice.com,rack=15,dc=02

Later you will be able to query either specific server, giving all tags, either ask for some aggregation:

    sum:out_speed{dc=02}

Query below will give you new time series with summary speed across whole datacenter.


**Data precision** Pulsedb is using "fixed point". It means that it stores only 13-14 significant
bits for number, so any value is packed into 2 bytes. It is considered enough for any reasonable measurement.


Structure
---------

Pulsedb can be used as an embeddable library and as a separate server that speaks custom protocol.
HTTP is used to open connection, so you can hide pulsedb server behind nginx.



Pulsedb library
===============


Include it as a rebar dependency. It depends on several libraries.
You can remove jsx,lhttpc,meck,cowboy from dependencies, because they are used only
in server mode.




Writing data
----------------------


    {ok, _} = pulsedb:open(my_database, "stats"), % Will call start_link
    pulsedb:append([
        {input, 1378950600, 432423, [{host,<<"host1.local">>,{iface,<<"eth0">>}}]},
        {output, 1378950600, 4324141232, [{host,<<"host1.local">>,{iface,<<"eth0">>}}]},

        {input, 1378950601, 435003, [{host,<<"host1.local">>,{iface,<<"eth0">>}}]},
        {output, 1378950601, 4324432132, [{host,<<"host1.local">>,{iface,<<"eth0">>}}]},

        {input, 1378950602, 442143, [{host,<<"host1.local">>,{iface,<<"eth0">>}}]},
        {output, 1378950602, 4328908132, [{host,<<"host1.local">>,{iface,<<"eth0">>}}]}
    ], my_database).


Also you always have preconfigured memory database:

    pulsedb:append({input, 1378950600, 432423, [{host,<<"host1.local">>,{iface,<<"eth0">>}}]}, memory).


It will be automatically downscaled to minute precision after 60 seconds and deleted after several hours.



Pulsedb will create files 

    stats/2013/09/12/config_v3
    stats/2013/09/12/data_v3

You can delete old files without any problems. It is designed so that data for each day is written independently
in different folders.


You can write timestamps in any order. If you want to upload delayed timestamps, no problems: pulsedb will write it.




Reading data
------------


    {ok, Ticks, _} = pulsedb:read("sum:output{from=2013-09-12,to=2013-09-13}", my_database),
    {ok, Ticks2, _} = pulsedb:read("sum:1m-avg:output{from=1378950600,to=1378950800,host=host1.local}", my_database).

Syntax of query is like in OpenTSDB



Subscribing
-----------

You can even subscribe to some query:

    pulsedb:subscribe("sum:output").

Mention, that you are subscribing only to those measurements that are going to memory.



Collecting
----------

You can ask pulsedb to start collector of data.

For example, you want to read network output. Write your own collector:


    -module(network_collector).
    -export([pulse_init/1, pulse_collect/1]).
    
    pulse_init(Iface) -> {ok, {Iface,gen_server:call(?MODULE, {output,Iface})}}.
    
    pulse_collect({Iface,Bytes1}) ->
      Bytes2 = gen_server:call(?MODULE, {output,Iface}),
      {reply, [{output, Bytes2-Bytes1, [{interface,Iface}]}], {Iface,Bytes2}}.


pulse_init will be called with some arguments (see below) and must return {ok, State}.
pulse_collect will be called once per second and previous state will be passed as an argument.
pulse_collect must return:

    {reply, [{Name,Value,[{TagName,TagValue}]}], State2}

Mention that you don't need to reply with UTC. Pulsedb will put it automatically.

Now you need to launch collector for your module:

    pulsedb:collect(<<"net.eth0">>, network_collector, <<"eth0">>, [{copy,flu_pulsedb}])

Look at the last option: {copy, To}. If you specify this option, pulsedb will write data not only to memory,
but also to some opened pulsedb file or network server.




Server mode
===========


Installation
------------

You can install this server from a debian package or compile it running: 

    make

Erlang R16B02 minimal is required.


Configuration
-------------

Next you need to copy config file: priv/pulsedb.config.sample to priv/pulsedb.config


Data collecting
---------------


Now you need to start collecting data.  

Pulsedb is using extension on top of HTTP to be able to hide behind nginx http server.

First you need to pass HTTP handshake with Upgrade header.

Client sends:

    CONNECT /api/v1/pulse_push HTTP/1.1
    Host: pulse.myservice.com
    Pulsedb-Api-Key: SOMEKEY
    Connection: Upgrade
    Upgrade: application/timeseries-text


Value of SOMEKEY will be explained below, but it is not required when no <code>{key, "...."}.</code> is set in config file.

Now server will send:

    HTTP 101 Upgraded
    upgrade: application/timeseries-text

If you see this, you have passed handshake. After this you need to use our special text-based protocol which is rather simple.

This protocol is statefull, so you must wrap socket it in some structure that will remember following things:

  * list of defined metrics
  * current base UTC

Base UTC is a UTC that will be a base for first sent metric. Usually you need to send:

    utc 1395139525

to socket. Server will not reply you anything, so continue sending data.

Next you need to define some metric in this connection:

    metrict 1 out_speed{interface=eth0,host=host1.myservice.com}

Value "1" is defined by client, must be integer and will be used later to identify metric data point.

After you have told base UTC and defined metric, you can send data to server:

    1 2 50

These numbers are read so:

    METRICT_ID DELTA_FROM_PREVIOUS_UTC VALUE

<code>DELTA_FROM_PREVIOUS_UTC</code> is not just a delta from base UTC, but it is an accumulated delta from previous UTC.
<code>VALUE</code> is a number with postfix: 0, 50, 10K, 45K, 2M, 250M, 10G, 40T. It is a simple way to reduce network traffic,
because usually if you want to tell how many megabytes, you are not interested in kilobytes.


So if you send series of metrics they can look so:

    utc 1395139525
    metrict 1 out_speed{interface=eth0,host=host1.myservice.com}
    metrict 2 out_speed{interface=eth0,host=host2.myservice.com}
    1 0 500K
    2 0 120K
    1 1 560K
    2 0 125K
    1 1 650K
    2 0 70K


The last command in this protocol is a ping. It MUST be used periodically to synchronize with remote server:

    ping

After this command client must wait for a "pong" response. After client receives pong response, it can be sure that
all data has been written.



Authenticating and data sharding
--------------------------------


Monitoring pushers
------------------


Data retrieving
---------------


Graphic resolver
----------------





