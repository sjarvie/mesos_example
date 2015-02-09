# !/bin/bash
echo "starting scheduler"
export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos-0.20.1.dylib

mvn clean compile exec:java \
  -DskipTests \
  -Pmvn \
  -Dexec.mainClass="org.zillabyte.MesosScheduler" \
  -Dexec.args="localhost:5050 /Users/sjarvie/mesos_example/run_executor.sh"
