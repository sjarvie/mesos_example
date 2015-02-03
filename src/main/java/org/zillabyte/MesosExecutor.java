package org.zillabyte;


import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
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

  private ExecutorService _threadpool;
  private ClassLoader _classloader;

  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    System.out.println("Registered executor on " + slaveInfo.getHostname());
    _classloader = this.getClass().getClassLoader();           
    Thread.currentThread().setContextClassLoader(_classloader);
    _threadpool = Executors.newCachedThreadPool();
  }
  
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    
    /** Notify Mesos that the task is now running **/
    TaskStatus status = TaskStatus.newBuilder()
        .setTaskId(task.getTaskId())
        .setState(TaskState.TASK_RUNNING).build();
    driver.sendStatusUpdate(status);
    
    /** Execute user code here */
    _threadpool.execute(new Runnable() {
      public void run() {
        
        /** Build a command **/
        CommandLine userEnvCl = new CommandLine("/bin/bash");
        userEnvCl.addArgument("-l");
        userEnvCl.addArgument("-c");
        userEnvCl.addArgument("python ~/http_server.py");
        
        Builder status = TaskStatus.newBuilder()
            .setTaskId(task.getTaskId());
        
        /** Execute the command */
        try {
          new DefaultExecutor().execute(userEnvCl);
          status.setState(TaskState.TASK_FINISHED);
        } catch (IOException e) {
          status.setState(TaskState.TASK_FAILED);
          e.printStackTrace();
        }
        
        /** Update the task status */
        driver.sendStatusUpdate(status.build());
        
      }
    });

  }

  /** This main method is run by script set by the scheduler, it starts the executor */
  public static void main(String[] args) {
    MesosExecutor exec = new MesosExecutor();
    new MesosExecutorDriver(exec).run();
  }
  
  /** Retrieve the script for starting the executor */
  public static ExecutorInfo getExecutorCommand() {
    
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
