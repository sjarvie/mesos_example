
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class MesosScheduler implements Scheduler {


  private final int CPUS = 1;
  private final int MEM = 128;

  /** Logger. */
  private static final Logger logger = LoggerFactory.getLogger(MesosScheduler.class);

  /** Number of instances to run. */
  private final int desiredInstances;

  /** List of pending instances. */
  private final List<String> pendingInstances = new ArrayList<>();

  /** List of running instances. */
  private final List<String> runningInstances = new ArrayList<>();

  /** Task ID generator. */
  private final AtomicInteger taskIDGenerator = new AtomicInteger();

  /** Constructor. */
  public MesosScheduler( int desiredInstances) {
    this.desiredInstances = desiredInstances;
  }

  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    logger.info("resourceOffers() with {} offers", offers.size());

    for (Protos.Offer offer : offers) {

      List<Protos.TaskInfo> tasks = new ArrayList<>();
      if (runningInstances.size() + pendingInstances.size() < desiredInstances) {

        /** Generate a unique taskID */
        Protos.TaskID taskId = Protos.TaskID.newBuilder()
            .setValue(Integer.toString(taskIDGenerator.incrementAndGet())).build();

        logger.info("Launching task {}", taskId.getValue());
        pendingInstances.add(taskId.getValue());

        /** Create the Task */
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .setExecutor(MesosExecutor.getExecutorCommand())
            .addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(CPUS)))
                .addResources(Protos.Resource.newBuilder()
                    .setName("mem")
                    .setType(Protos.Value.Type.SCALAR)
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(MEM)))
                    .build();

        tasks.add(task);
      }
      driver.launchTasks(Lists.newArrayList(offer.getId()), tasks);
    }

  }

  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    logger.info("Received status update " + status.getState());
  }

  public void registered(SchedulerDriver driver, FrameworkID frameworkID, MasterInfo masterInfo) {
    logger.info("Registered " + frameworkID);
  }


  public static void main(String[] args){
    FrameworkInfo framework = FrameworkInfo.newBuilder()
        .setName("ZillabyteMesosExecutorExample")
        .setUser("")
        .setRole("*")
        .build();

    String mesosAddress = args[0];
    String executorScriptPath = args[1];
    System.setProperty("executor_script_path",executorScriptPath);
    MesosScheduler scheduler = new MesosScheduler(1);
    MesosSchedulerDriver driver = new MesosSchedulerDriver(scheduler, framework, mesosAddress);
    driver.run();
  }

  public void disconnected(SchedulerDriver driver) {
  }

  public void error(SchedulerDriver driver, String message) {
  }

  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID, int status) {
  }

  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID, byte[] data) {
  }

  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
  }

  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
  }

  public void slaveLost(SchedulerDriver driver, SlaveID slaveID) {    
  }

}
