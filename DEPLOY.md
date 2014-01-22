Deploy instructions
===================



    apt-get install erlang-nox=1:16.b.2
    
    # Or if not available
    # wget http://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
    # dpkg -i erlang-solutions_1.0_all.deb
    # apt-get update
    # apt-get install esl-erlang=1:16.b.2
    
    apt-get install git
    adduser --disabled-password --gecos "" central
    mkdir -p /home/central/.ssh
    cp /root/.ssh/authorized_keys /home/central/.ssh/
    chown central.central -R /home/central
    chmod 0400 /home/central/.ssh/

    echo "deb http://nginx.org/packages/ubuntu/ precise nginx" > /etc/apt/sources.list.d/nginx.list 
    apt-get update
    apt-get -y --force-yes install nginx htop


then as central

    git clone git://github.com/flussonic/pulsedb.git
    cd pulsedb
    ./rebar get-deps compile
    cp priv/pulsedb.config.sample priv/pulsedb.config


edit priv/pulsedb.config

    ./run.sh # in screen


