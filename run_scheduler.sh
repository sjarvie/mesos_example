# !/bin/bash
echo "starting scheduler"
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
#update the path to point to jar
java -cp /Users/sjarvie/mesos_example/target/Mesos-0.0.1-SNAPSHOT.jar org.zillabyte.MesosScheduler localhost:5050 /Users/sjarvie/mesos_example/run_executor.sh
