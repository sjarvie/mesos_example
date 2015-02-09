package org.zillabyte;


import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskStatus.Builder;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;

public class MesosExecutor implements Executor {

  private ClassLoader _classloader;

  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    System.out.println("Registered executor on " + slaveInfo.getHostname());
    _classloader = this.getClass().getClassLoader();           
    Thread.currentThread().setContextClassLoader(_classloader);
  }

  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    Builder status = TaskStatus.newBuilder()
        .setTaskId(task.getTaskId());
    
    /** Notify Mesos that the task is now running **/
    status.setState(TaskState.TASK_RUNNING);
    driver.sendStatusUpdate(status.build());

    /** Run the task */
    try {
      new DefaultExecutor().execute(pythonServerCommand());
      status.setState(TaskState.TASK_FINISHED);
    } catch (IOException e) {
      status.setState(TaskState.TASK_FAILED);
      e.printStackTrace();
    } 

    /** Update the task status */
    driver.sendStatusUpdate(status.build());
  }

  /** This main method is run by script set by the scheduler, it starts the executor */
  public static void main(String[] args) {
    MesosExecutor exec = new MesosExecutor();
    new MesosExecutorDriver(exec).run();
  }

  public static CommandLine pythonServerCommand(){
    /** Build a command to download and spawn a basic python server **/
    return new CommandLine("/bin/bash")
    .addArgument("-l")
    .addArgument("-c")
    .addArgument("wget https://gist.githubusercontent.com/sjarvie/d697b0835057e5e5a029/raw/11427e7629d9fb70ce7608f6dfb26e0a2aeed53c/http.py -O http.py; python http.py", false);

  }

  /** Retrieve the script for starting the executor */
  public static ExecutorInfo getExecutorScript() {
    String scriptPath = System.getProperty("executor_script_path","~/run-executor.sh");
    return ExecutorInfo.newBuilder()
        . setCommand(CommandInfo.newBuilder().setValue("/bin/sh "+ scriptPath))
        .setExecutorId(ExecutorID.newBuilder().setValue("zillabyte-mesos"))
        .build();
  }

  public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
  }

  public void disconnected(ExecutorDriver driver) {
  }

  public void killTask(ExecutorDriver driver, TaskID taskId) {
  }

  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
  }

  public void shutdown(ExecutorDriver driver) {
  }

  public void error(ExecutorDriver driver, String message) {
  }

}
