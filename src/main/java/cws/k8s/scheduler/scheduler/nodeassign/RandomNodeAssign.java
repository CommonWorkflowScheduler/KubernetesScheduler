package cws.k8s.scheduler.scheduler.nodeassign;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class RandomNodeAssign extends NodeAssign {

    @Override
    public List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode ) {
        LinkedList<NodeTaskAlignment> alignment = new LinkedList<>();
        final ArrayList<Map.Entry<NodeWithAlloc, Requirements>> entries = new ArrayList<>( availableByNode.entrySet() );
        for ( final Task task : unscheduledTasks ) {
            log.debug("Pod: " + task.getPod().getName() + " Requested Resources: " + task.getPlanedRequirements() );
            Collections.shuffle( entries );
            boolean assigned = false;
            int nodesTried = 0;
            for ( Map.Entry<NodeWithAlloc, Requirements> e : entries ) {
                final NodeWithAlloc node = e.getKey();
                if ( scheduler.canScheduleTaskOnNode( availableByNode.get( node ), task, node ) ) {
                    nodesTried++;
                    alignment.add(new NodeTaskAlignment( node, task));
                    availableByNode.get( node ).subFromThis(task.getPlanedRequirements());
                    log.debug("--> " + node.getName());
                    assigned = true;
                    task.getTraceRecord().foundAlignment();
                    break;
                }
            }
            task.getTraceRecord().setSchedulerNodesTried( nodesTried );
            if ( !assigned ) {
                log.trace( "No node with enough resources for {}", task.getPod().getName() );
            }
        }
        return alignment;
    }

}
