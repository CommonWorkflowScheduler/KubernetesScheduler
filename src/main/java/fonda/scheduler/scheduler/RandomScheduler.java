package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class RandomScheduler extends Scheduler {

    public RandomScheduler( String execution,
                            KubernetesClient client,
                            String namespace,
                            SchedulerConfig config ) {
        super(execution, client, namespace, config);
    }

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        List<NodeTaskAlignment> alignment = new LinkedList<>();

        for ( final Task task : unscheduledTasks ) {
            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest());

            final Set<NodeWithAlloc> matchingNodes = getMatchingNodesForTask(availableByNode,task);
            if( !matchingNodes.isEmpty() ) {

                Optional<NodeWithAlloc> node = matchingNodes.stream().findAny();
                if( node.isPresent() ) {
                    alignment.add(new NodeTaskAlignment(node.get(), task));
                    availableByNode.get(node.get()).subFromThis(pod.getRequest());
                    log.info("--> " + node.get().getName());
                }

            } else {
                log.trace( "No node with enough resources for {}", pod.getName() );
            }

        }
        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( true );
        return scheduleObject;
    }

}
