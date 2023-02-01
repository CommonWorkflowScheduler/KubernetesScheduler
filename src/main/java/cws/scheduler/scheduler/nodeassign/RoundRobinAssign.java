package fonda.scheduler.scheduler.nodeassign;

import fonda.scheduler.client.Informable;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.PodWithAge;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.model.Task;
import fonda.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

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
            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest() );
            synchronized ( this ) {
                int firstTrial = nextNode;
                do {
                    final NodeWithAlloc node = nodes.get( nextNode );
                    log.info( "Next node: " + node.getName() + "--( " + nextNode + " )" );
                    nextNode = ( nextNode + 1 ) % nodes.size();
                    if ( scheduler.canSchedulePodOnNode( availableByNode.get( node ), pod, node ) ) {
                        alignment.add( new NodeTaskAlignment( node, task ) );
                        availableByNode.get( node ).subFromThis( pod.getRequest() );
                        log.info( "--> " + node.getName() );
                        break;
                    }
                } while ( nextNode != firstTrial );
            }
        }
        return alignment;
    }

    @Override
    public void informResourceChange() {}

    @Override
    public void newNode( NodeWithAlloc node ) {
        synchronized ( this ) {
            if ( nodes != null & !nodes.contains( node ) ) {
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
