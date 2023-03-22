package cws.k8s.scheduler.scheduler.outlabel;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;

import java.util.*;

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
        if ( outLabel == null ) {
            return null;
        }
        final InternalHolder holder = internalHolder.get(outLabel);
        return holder == null ? null : holder.getBestNode().stream().findFirst().orElse(null);
    }

    public void scheduleTaskOnNode(Task task, NodeLocation node ){
        final String outLabel;
        if ( (outLabel = task.getOutLabel()) == null ) {
            return;
        }
        internalHolder.computeIfAbsent( outLabel, key -> create() ).addTask( task, node );
    }

    /**
     * @return new Instance of the required Holder
     */
    protected abstract InternalHolder create();

}
