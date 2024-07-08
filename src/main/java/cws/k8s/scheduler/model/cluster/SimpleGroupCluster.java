package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.location.NodeLocation;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class SimpleGroupCluster extends GroupCluster {

    private final KubernetesClient client;

    @Override
    void recalculate() {
        final List<NodeWithAlloc> allNodes = client.getAllNodes();

        // Filter labels that have waiting tasks.
        final List<Map.Entry<String, LabelCount>> labelsWithWaitingTasks = countPerLabel
                .entrySet()
                .stream()
                // If nothing waiting, we do not need a suitable node, so we do not reassign it
                // If there is only one task with a label it can run where ever resources are available. Do not force a location.
                .filter( kv -> kv.getValue().getCountWaiting() > 0 && kv.getValue().getCount() > 1 )
                // Sort reverse by count
                .sorted( Comparator.comparingInt( kv -> - kv.getValue().getCount() ) )
                .collect( Collectors.toList() );

        // store how many tasks would have been executed on every with the current alignment
        // this is an approximation since tasks can have multiple labels and would appear multiple times
        Map<NodeLocation,Integer> tasksOnNode = new HashMap<>();

        // For every label and node count how many tasks could run (affinities match), we ignore the current load completely
        for ( Map.Entry<String, LabelCount> labelWithWaitingTasks : labelsWithWaitingTasks ) {
            Queue<TasksOnNodeWrapper> xTasksCanRunOnNode = new PriorityQueue<>();
            for ( NodeWithAlloc node : allNodes ) {
                final long count = labelWithWaitingTasks
                        .getValue()
                        .getWaitingTasks()
                        .stream()
                        .filter( task -> node.affinitiesMatch( task.getPod() ) )
                        .count();
                if ( count > 0 ) {
                    final TasksOnNodeWrapper tasksOnNodeWrapper = new TasksOnNodeWrapper( node.getNodeLocation(), (int) count );
                    xTasksCanRunOnNode.add( tasksOnNodeWrapper );
                }
            }
            if ( !xTasksCanRunOnNode.isEmpty() ) {
                final NodeLocation nodeLocation = calculateBestFittingNode( labelWithWaitingTasks.getKey(), xTasksCanRunOnNode, labelWithWaitingTasks.getValue(), tasksOnNode );
                if ( nodeLocation != null ) {
                    addNodeToLabel( nodeLocation, labelWithWaitingTasks.getKey() );
                    tasksOnNode.put( nodeLocation, tasksOnNode.getOrDefault( nodeLocation, 0 ) + labelWithWaitingTasks.getValue().getCountWaiting() );
                }
            }
        }
    }

    /**
     * This method first looks which nodes ran the most tasks with the current label already and then selects the node where the most tasks can run.
     * @param label
     * @param xTasksCanRunOnNode
     * @param labelCount
     * @param tasksOnNode
     * @return
     */
    private NodeLocation calculateBestFittingNode( String label, Queue<TasksOnNodeWrapper> xTasksCanRunOnNode, LabelCount labelCount, Map<NodeLocation, Integer> tasksOnNode ) {
        if ( xTasksCanRunOnNode.isEmpty() ) {
            return null;
        }
        final Set<NodeLocation> bestFittingNodes = new HashSet<>();
        final TasksOnNodeWrapper bestFittingNode = xTasksCanRunOnNode.poll();
        bestFittingNodes.add( bestFittingNode.getNode() );
        while( !xTasksCanRunOnNode.isEmpty() && xTasksCanRunOnNode.peek().getShare() == bestFittingNode.getShare() ){
            bestFittingNodes.add( xTasksCanRunOnNode.poll().getNode() );
        }

        final List<TasksOnNodeWrapper> runningOrfinishedOnNodes = new ArrayList<>(labelCount.getRunningOrfinishedOnNodes());
        if ( runningOrfinishedOnNodes.isEmpty() ) {
            // if tass with this label have not been executed
            return findBestFittingNode( bestFittingNodes, tasksOnNode );
        }

        for ( TasksOnNodeWrapper tasksOnNodeWrapper : runningOrfinishedOnNodes ) {
            if ( bestFittingNodes.contains( tasksOnNodeWrapper.getNode() ) ) {
                return tasksOnNodeWrapper.getNode();
            }
        }

        final NodeLocation nodeLocation = calculateBestFittingNode( label, xTasksCanRunOnNode, labelCount, tasksOnNode );
        if ( nodeLocation != null ) {
            return nodeLocation;
        }
        // If no node is found, return a random one
        return findBestFittingNode( bestFittingNodes, tasksOnNode );

    }

    /**
     * Assign to the node that has the least tasks to process yet
     * @param bestFittingNodes list of potential nodes
     * @param tasksOnNode map of tasks on each node
     * @return
     */
    private NodeLocation findBestFittingNode( final Set<NodeLocation> bestFittingNodes, Map<NodeLocation, Integer> tasksOnNode ) {
        NodeLocation best = null;
        for ( NodeLocation fittingNode : bestFittingNodes ) {
            if ( best == null || tasksOnNode.getOrDefault( fittingNode, 0 ) < tasksOnNode.getOrDefault( best, 0 ) ) {
                best = fittingNode;
            }
        }
        return best;
    }

}
