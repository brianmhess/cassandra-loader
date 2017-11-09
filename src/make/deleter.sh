#!/bin/sh

cat src/make/cassandra-loader.sh build/libs/cassandra-deleter-uber*.jar > build/cassandra-deleter && chmod 755 build/cassandra-deleter
