#!/bin/sh

wget http://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
dpkg -i erlang-solutions_1.0_all.deb


# erlang-base erlang-ssl erlang-tools
apt-get -y install erlang-nox


