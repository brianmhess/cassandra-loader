#!/bin/sh
MYSELF=`which "$0" 2>/dev/null`
[ $? -gt 0 -a -f "$0" ] && MYSELF="./$0"
java=java
if test -n "$JAVA_HOME"; then
    java="$JAVA_HOME/bin/java"
fi
exec "$java" -XX:+UseG1GC -Xmx1G -Xms1G -XX:+UseTLAB -XX:+ResizeTLAB $java_args -jar $MYSELF "$@"
exit 1 
