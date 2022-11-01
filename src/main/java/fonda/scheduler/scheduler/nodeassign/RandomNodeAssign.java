package fonda.scheduler.scheduler.nodeassign;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.PodWithAge;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.model.Task;
import fonda.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;

@Slf4j
public class RandomNodeAssign extends NodeAssign {

    @Override
    public List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode ) {
        LinkedList<NodeTaskAlignment> alignment = new LinkedList<>();
        final ArrayList<Map.Entry<NodeWithAlloc, Requirements>> entries = new ArrayList<>( availableByNode.entrySet() );
        for ( final Task task : unscheduledTasks ) {
            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest() );
            Collections.shuffle( entries );
            boolean assigned = false;
            for ( Map.Entry<NodeWithAlloc, Requirements> e : entries ) {
                final NodeWithAlloc node = e.getKey();
                if ( scheduler.canSchedulePodOnNode( availableByNode.get( node ), pod, node ) ) {
                    alignment.add(new NodeTaskAlignment( node, task));
                    availableByNode.get( node ).subFromThis(pod.getRequest());
                    log.info("--> " + node.getName());
                    assigned = true;
                    break;
                }
            }
            if ( !assigned ) {
                log.trace( "No node with enough resources for {}", pod.getName() );
            }
        }
        return alignment;
    }

}
