package fonda.scheduler.util;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.NodeLocation;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CopyToNodeManager {

    private final Map<NodeLocation, List<Task>> taskOnNodes = new HashMap<>();
    private final Map<Task, List<NodeLocation>> nodesForTask = new HashMap<>();

    public void copyToNode( Task task, NodeLocation node ){
        synchronized ( this ) {
            List<Task> tasks = taskOnNodes.get( node );
            if ( tasks == null ) {
                tasks = new LinkedList<>();
                taskOnNodes.put( node, tasks );
            }
            tasks.add( task );

            List<NodeLocation> nodes = nodesForTask.get( task );
            if ( nodes == null ) {
                nodes = new LinkedList<>();
                nodesForTask.put( task, nodes );
            }
            nodes.add( node );
        }
    }

    public void finishedCopyToNode( Task task, NodeLocation node ){
        synchronized ( this ) {
            List<Task> tasks = taskOnNodes.get( node );
            if ( tasks != null ) {
                tasks.remove( task );
            }

            List<NodeLocation> nodes = nodesForTask.get( task );
            if ( nodes != null ) {
                nodes.remove( node );
            }
        }
    }

    public Map<NodeLocation, Integer> getCurrentlyCopyingTasksOnNode() {
        Map<NodeLocation, Integer> result = new HashMap<>();
        for ( Map.Entry<NodeLocation, List<Task>> entry : taskOnNodes.entrySet() ) {
            result.put( entry.getKey(), entry.getValue().size() );
        }
        return result;
    }

}
