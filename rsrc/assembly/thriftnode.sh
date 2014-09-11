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

# Specifies any additional arguments to the JVM.
JVM_OPTS="-XX:+UseParallelGC"

# GC logging options -- uncomment to enable (this is not the exact PID; but close)
#JVM_OPTS="$JVM_OPTS -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:$HOME/$HOSTNAME-$$.vgc"

# Remote JMX listener -- uncomment to enable
#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.port=9001 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"

# start the program
$JAVA -Djava.library.path=$PRGDIR/lib -cp $PRGDIR/lib:$CLASSPATH -Xms2G -Xmx2G $JVM_OPTS ch.usi.da.paxos.ThriftNode $@
