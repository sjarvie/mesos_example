package org.zillabyte;


import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;

public class MesosHelper {

  
  public static Resource scalarResouce(String key, int value){
    Resource cpus = Resource.newBuilder()
          .setName(key)
          .setType(Type.SCALAR)
          .setScalar(Scalar.newBuilder().setValue(value).build()).build();
    return cpus;

  }
}
