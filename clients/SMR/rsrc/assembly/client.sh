#!/bin/sh

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

# start the program
$JAVA -cp $PRGDIR/lib:$CLASSPATH ch.usi.da.smr.Client $@
