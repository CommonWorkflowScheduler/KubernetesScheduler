package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.cluster.GroupCluster;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.la2.ready2run.ReadyToRunToNode;

import java.util.List;

public class LocationAwareSchedulerGroups extends LocationAwareSchedulerV2 {

    GroupCluster groupCluster;

    public LocationAwareSchedulerGroups( String name, KubernetesClient client, String namespace, SchedulerConfig config, InputAlignment inputAlignment, ReadyToRunToNode readyToRunToNode ) {
        super( name, client, namespace, config, inputAlignment, readyToRunToNode );
    }

    @Override
    protected void tasksWhereAddedToQueue( List<Task> newTasks ){
        super.tasksWhereAddedToQueue( newTasks );
        groupCluster.tasksBecameAvailable( newTasks );
    }

    @Override
    void taskWasScheduled(Task task ) {
        super.taskWasScheduled( task );
        groupCluster.taskWasAssignedToNode( task );
    }


}
