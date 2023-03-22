package cws.k8s.scheduler.util.copying;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.util.AlignmentWrapper;
import cws.k8s.scheduler.util.FilePath;
import lombok.ToString;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of what is currently being copied to all node and which copy tasks are running.
 * This is used to avoid copying the same data to a node multiple times and to overload a single node with copy tasks.
 */
@ToString
public class CurrentlyCopying {

    private final Map<NodeLocation, CurrentlyCopyingOnNode> copyingToNode = new ConcurrentHashMap<>();
    private final Map<NodeLocation, List<Task>> taskOnNodes = new HashMap<>();
    private final Map<Task, List<NodeLocation>> nodesForTask = new HashMap<>();

    public void add( Task task, NodeLocation nodeLocation, CurrentlyCopyingOnNode currentlyCopyingOnNode ) {
        if ( currentlyCopyingOnNode == null || currentlyCopyingOnNode.isEmpty() ) {
            return;
        }
        copyingToNode.compute( nodeLocation, ( node, currentlyCopying ) -> {
            if ( currentlyCopying == null ) {
                return currentlyCopyingOnNode;
            } else {
                currentlyCopying.add( currentlyCopyingOnNode );
                return currentlyCopying;
            }
        } );
        copyToNode( task, nodeLocation );
    }

    public CurrentlyCopyingOnNode get( NodeLocation nodeLocation ) {
        return copyingToNode.computeIfAbsent( nodeLocation, node -> new CurrentlyCopyingOnNode() );
    }

    public void remove( Task task, NodeLocation nodeLocation, CurrentlyCopyingOnNode currentlyCopyingOnNode ) {
        if ( currentlyCopyingOnNode == null || currentlyCopyingOnNode.isEmpty() ) {
            return;
        }
        copyingToNode.compute( nodeLocation, ( node, currentlyCopying ) -> {
            if ( currentlyCopying == null ) {
                return null;
            } else {
                currentlyCopying.remove( currentlyCopyingOnNode );
                return currentlyCopying;
            }
        } );
        finishedCopyToNode( task, nodeLocation );
    }

    private void copyToNode( Task task, NodeLocation node ){
        synchronized ( this ) {
            List<Task> tasks = taskOnNodes.computeIfAbsent( node, x -> new LinkedList<>() );
            tasks.add( task );

            List<NodeLocation> nodes = nodesForTask.computeIfAbsent( task, x -> new LinkedList<>() );
            nodes.add( node );
        }
    }

    private void finishedCopyToNode( Task task, NodeLocation node ){
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
    
    public List<Task> getTasksOnNode( NodeLocation nodeLocation ) {
        synchronized ( this ) {
            return taskOnNodes.getOrDefault( nodeLocation, new LinkedList<>() );
        }
    }

    public int getNumberOfNodesForTask( Task task ) {
        final List<NodeLocation> nodeLocations;
        synchronized ( this ) {
            nodeLocations = nodesForTask.get( task );
        }
        return nodeLocations == null ? 0 : nodeLocations.size();
    }

    public Map<NodeLocation, Integer> getCurrentlyCopyingTasksOnNode() {
        Map<NodeLocation, Integer> result = new HashMap<>();
        for ( Map.Entry<NodeLocation, List<Task>> entry : taskOnNodes.entrySet() ) {
            result.put( entry.getKey(), entry.getValue().size() );
        }
        return result;
    }

    public void addAlignment( final Map<Location, AlignmentWrapper> nodeFileAlignment, Task task, NodeWithAlloc node ) {
        for (Map.Entry<Location, AlignmentWrapper> entry : nodeFileAlignment.entrySet()) {
            final CurrentlyCopyingOnNode map = get(node.getNodeLocation());
            for ( FilePath filePath : entry.getValue().getFilesToCopy()) {
                if ( entry.getKey() != node.getNodeLocation() ) {
                    map.add( filePath.getPath(), task, entry.getKey() );
                }
            }
        }
    }



}
