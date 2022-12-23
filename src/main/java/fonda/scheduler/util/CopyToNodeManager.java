package fonda.scheduler.util;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CopyToNodeManager {

    private final Map<NodeWithAlloc, List<Task>> taskOnNodes = new HashMap<>();
    private final Map<Task, List<NodeWithAlloc>> nodesForTask = new HashMap<>();

    public void copyToNode( Task task, NodeWithAlloc node, double part ){
        synchronized ( this ) {
            List<Task> tasks = taskOnNodes.get( node );
            if ( tasks == null ) {
                tasks = new LinkedList<>();
                taskOnNodes.put( node, tasks );
            }
            tasks.add( task );

            List<NodeWithAlloc> nodes = nodesForTask.get( task );
            if ( nodes == null ) {
                nodes = new LinkedList<>();
                nodesForTask.put( task, nodes );
            }
            nodes.add( node );
        }
    }

    public void finishedCopyToNode( Task task, NodeWithAlloc node ){
        synchronized ( this ) {
            List<Task> tasks = taskOnNodes.get( node );
            if ( tasks != null ) {
                tasks.remove( task );
            }

            List<NodeWithAlloc> nodes = nodesForTask.get( task );
            if ( nodes != null ) {
                nodes.remove( node );
            }
        }
    }

    public Map<NodeWithAlloc, Integer> getCurrentlyCopyingTasksOnNode() {
        Map<NodeWithAlloc, Integer> result = new HashMap<>();
        for ( Map.Entry<NodeWithAlloc, List<Task>> entry : taskOnNodes.entrySet() ) {
            result.put( entry.getKey(), entry.getValue().size() );
        }
        return result;
    }

}
