#!/bin/sh

cat cassandra-loader.sh build/libs/cassandra-loader-uber*.jar > build/cassandra-loader && chmod 755 build/cassandra-loader
