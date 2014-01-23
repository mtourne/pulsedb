Deploy instructions
===================




wget http://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
dpkg -i erlang-solutions_1.0_all.deb
echo "deb http://nginx.org/packages/ubuntu/ precise nginx" > /etc/apt/sources.list.d/nginx.list 
wget -q -O - http://nginx.org/keys/nginx_signing.key | apt-key add -
apt-get update

apt-get -y install erlang-nox=1:16.b.2 git nginx htop
adduser --disabled-password --gecos "" central
mkdir -p /home/central/.ssh
cp /root/.ssh/authorized_keys /home/central/.ssh/
chown central.central -R /home/central
chmod 0700 /home/central/.ssh/


# then as central

git clone git://github.com/flussonic/pulsedb.git
cd pulsedb
./rebar get-deps compile
cp priv/pulsedb.config.sample priv/pulsedb.config


# edit priv/pulsedb.config

./run.sh # in screen


