#!/bin/sh

wget http://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
dpkg -i erlang-solutions_1.0_all.deb


apt-get update
# apt-get -y install erlang-nox=1:16.b.2
apt-get -y install erlang-base erlang-ssl erlang-tools


