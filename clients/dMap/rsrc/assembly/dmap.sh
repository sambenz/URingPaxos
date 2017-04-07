#!/bin/sh

HOSTNAME=`hostname`

# Setup the JVM
if [ "x$JAVA" = "x" ]; then
  if [ "x$JAVA_HOME" != "x" ]; then
    JAVA="$JAVA_HOME/bin/java"
  else
  JAVA="java"
  fi
fi

# find directory where this file is located 
PRGDIR=`dirname "$0"`

for i in $PRGDIR/lib/*.jar; do
  CLASSPATH="$CLASSPATH":"$i"
done

# GC logging options -- uncomment to enable (this is not the exact PID; but close)
#JVM_OPTS="$JVM_OPTS -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:$HOME/$HOSTNAME-$$.vgc"

# start the program
$JAVA -cp $PRGDIR/lib:$CLASSPATH -Xms3G -Xmx3G $JVM_OPTS ch.usi.da.dmap.server.DMapReplica $@
