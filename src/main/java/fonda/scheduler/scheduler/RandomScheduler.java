package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class RandomScheduler extends SchedulerWithDaemonSet {

    public RandomScheduler(String name, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(name, client, namespace, config);
    }

    @Override
    public int schedule( final List<Task> unscheduledTasks ) {
        log.info("Schedule " + this.getName());
        List<NodeWithAlloc> items = getNodeList();
        int unscheduled = 0;
        for ( final Task task : unscheduledTasks) {
            if(isClose()) return -1;
            final Pod pod = task.getPod();
            Optional<NodeWithAlloc> node = items.stream().filter(x -> x.canSchedule(pod) && this.getDaemonOnNode(x) != null).findFirst();
            if( node.isPresent() ){
                log.info("Task needs: " + task.getConfig().getInputs().toString());
                assignTaskToNode( task, node.get() );
                super.taskWasScheduled( task );
            } else {
                log.info( "No node with enough resources for {}", pod.getMetadata().getName() );
                unscheduled++;
            }
        }
        return unscheduled;
    }

}
