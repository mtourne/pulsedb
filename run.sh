#!/bin/bash

ERL_LIBS=..:deps erl -name pulsedb@127.0.0.1 -pa ebin -s pulsedb_launcher

