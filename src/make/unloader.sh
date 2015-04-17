#!/bin/sh

cat src/make/cassandra-loader.sh build/libs/cassandra-unloader-uber*.jar > build/cassandra-unloader && chmod 755 build/cassandra-unloader
