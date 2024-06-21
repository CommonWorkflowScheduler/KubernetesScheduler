package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.Task;

import java.util.List;

public interface GroupCluster {

    void tasksBecameAvailable( List<Task> tasks );

    void taskWasAssignedToNode( Task task );

}
