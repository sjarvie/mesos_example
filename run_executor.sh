#!/bin/sh
echo "starting Executor"
export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos-0.20.1.dylib

cd /Users/sjarvie/mesos_example/
mvn clean compile exec:java \
  -DskipTests \
  -Pmvn \
  -Dexec.mainClass="org.zillabyte.MesosExecutor" 
