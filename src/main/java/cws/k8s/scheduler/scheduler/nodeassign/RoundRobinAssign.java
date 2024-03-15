package cws.k8s.scheduler.scheduler.nodeassign;

import cws.k8s.scheduler.client.Informable;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class RoundRobinAssign extends NodeAssign implements Informable {

    private ArrayList<NodeWithAlloc> nodes = null;
    private int nextNode = 0;

    @Override
    public List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode ) {
        synchronized ( this ) {
            if ( nodes == null ) {
                nodes = new ArrayList<>( availableByNode.keySet() );
            }
        }

        LinkedList<NodeTaskAlignment> alignment = new LinkedList<>();
        for ( final Task task : unscheduledTasks ) {
            log.info("Pod: " + task.getPod().getName() + " Requested Resources: " + task.getPlanedRequirements() );
            int nodesTried = 0;
            synchronized ( this ) {
                int firstTrial = nextNode;
                nodesTried++;
                do {
                    final NodeWithAlloc node = nodes.get( nextNode );
                    log.info( "Next node: " + node.getName() + "--( " + nextNode + " )" );
                    nextNode = ( nextNode + 1 ) % nodes.size();
                    if ( scheduler.canScheduleTaskOnNode( availableByNode.get( node ), task, node ) ) {
                        alignment.add( new NodeTaskAlignment( node, task ) );
                        availableByNode.get( node ).subFromThis( task.getPlanedRequirements() );
                        log.info( "--> " + node.getName() );
                        task.getTraceRecord().foundAlignment();
                        break;
                    }
                } while ( nextNode != firstTrial );
            }
            task.getTraceRecord().setSchedulerNodesTried( nodesTried );
        }
        return alignment;
    }

    @Override
    public void informResourceChange() {}

    @Override
    public void newNode( NodeWithAlloc node ) {
        synchronized ( this ) {
            if ( nodes != null && !nodes.contains( node ) ) {
                nodes.add( node );
            }
        }
    }

    @Override
    public void removedNode( NodeWithAlloc node ) {
        synchronized ( this ) {
            if ( nodes != null ) {
                final int index = nodes.indexOf( node );
                if ( index != -1 ) {
                    nodes.remove( index );
                    if ( index < nextNode ) {
                        nextNode--;
                    }
                }
            }
        }
    }
}
