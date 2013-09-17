Pulsedb
=======


This library is a storage for statistics like network usage, load, etc..

It is an append-only online-compress storage, that supports indexing and simple lookups of stored data.


Writing data
----------------------


    {ok, DB1} = pulsedb:open("stats"),
    {ok, DB2} = pulsedb:append([
        #tick{name = <<"net/eth0">>, utc = 1378950600, values = [{input,432423},{output,4324141232}]},
        #tick{name = <<"net/eth0">>, utc = 1378950601, values = [{input,435003},{output,4324432132}]},
        #tick{name = <<"net/eth0">>, utc = 1378950602, values = [{input,442143},{output,4328908132}]},
        #tick{name = <<"net/eth0">>, utc = 1378950603, values = [{input,454643},{output,4324009544}]}
    ], DB1),
    pulsedb:close(DB2).


Pulsedb will create files 

    stats/2013/09/12/config_v1
    stats/2013/09/12/data_v1
    stats/2013/09/12/index_v1

You can delete old files without any problems. It is designed so that data for each day is written independently
in different folders.

Also you should know that pulsedb is designed for batch write. It is recommended to push data once
per minute so that pulsedb will be able to compress data.

! Warning! You must always write monotonic increasing timestamps and one source name in one batch.



Reading data
------------


    {ok, DB1} = pulsedb:open("stats"),
    {ok, Ticks, DB2} = pulsedb:read([{name,<<"net/eth0">>},{from,"2013-09-12"},{to,"2013-09-12 12:45:32"}]),
    pulsedb:close(DB2).


Pulsedb can read data for several days. It will use created index for this.


