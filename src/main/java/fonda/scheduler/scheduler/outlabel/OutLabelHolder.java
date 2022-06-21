package fonda.scheduler.scheduler;

import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.NodeLocation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The holder cannot be updated, if scheduling for a task fails.
 */
public abstract class OutLabelHolder {

    protected HashMap<String, InternalHolder> internalHolder = new HashMap<>();

    /**
     * Get Node with most Tasks running on it
     * @param outLabel
     * @return Null if no node was determined until now
     */
    public NodeLocation getNodeForLabel(String outLabel) {
        final InternalHolder holder = internalHolder.get(outLabel);
        return ( holder == null ) ? null : holder.getBestNode();
    }

    public void scheduleTaskOnNode(Task task, NodeLocation node ){
        final String outLabel;
        if ( (outLabel = task.getOutLabel()) == null ) return;
        internalHolder.computeIfAbsent( outLabel, key -> new InternalHolder( this ) ).addTask( task, node );
    }

    abstract protected NodeLocation determineBestNode( Set<Map.Entry<NodeLocation, Set<Task>>> set );

    private class InternalHolder {
        private final OutLabelHolder outLabelHolder;
        private final Map<NodeLocation, Set<Task>> tasksByNode = new HashMap<>();

        public InternalHolder( OutLabelHolder outLabelHolder ) {
            this.outLabelHolder = outLabelHolder;
        }

        public NodeLocation getBestNode() {
            return outLabelHolder.determineBestNode( tasksByNode.entrySet() );
        }

        public void addTask( Task task, NodeLocation node ) {
            tasksByNode.computeIfAbsent( node, key -> new HashSet<>() ).add( task );
        }

    }

}
