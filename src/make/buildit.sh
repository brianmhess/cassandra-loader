#!/bin/sh

cat src/make/cassandra-loader.sh build/libs/cassandra-loader-uber*.jar > build/cassandra-loader && chmod 755 build/cassandra-loader
