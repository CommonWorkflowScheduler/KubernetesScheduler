package cws.k8s.scheduler.scheduler.la2.capacityavailable;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.la2.TaskStat;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;
import cws.k8s.scheduler.util.SortedList;
import cws.k8s.scheduler.util.TaskStats;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SimpleCapacityAvailableToNode extends CapacityAvailableToNode {

    public SimpleCapacityAvailableToNode(
            CurrentlyCopying currentlyCopying,
            InputAlignment inputAlignment,
            int copySameTaskInParallel ) {
        super( currentlyCopying, inputAlignment, copySameTaskInParallel );
    }

    public List<NodeTaskFilesAlignment> createAlignmentForTasksWithEnoughCapacity(
            final TaskStats taskStats,
            final CurrentlyCopying planedToCopy,
            final Map<NodeWithAlloc, Requirements> availableByNodes,
            final List<NodeWithAlloc> allNodes,
            final int maxCopyingTaskPerNode,
            final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode,
            final int prio
    ) {


        List<NodeTaskFilesAlignment> nodeTaskAlignments = new LinkedList<>();

        //Remove available resources if a copy task is already running: this logic may not be optimal for more than 2 parallel copy tasks (unclear which task starts first)
        removeAvailableResources( taskStats, availableByNodes, allNodes );
        //Sort tasks by missing data: prefer tasks where the least data is missing on the node
        final SortedList<TaskStat> stats = new SortedList<>( taskStats.getTaskStats().stream().filter( TaskStat::missingDataOnAnyNode ).collect( Collectors.toList() ) );
        removeTasksThatAreCopiedMoreThanXTimeCurrently( stats, copySameTaskInParallel );

        while( !stats.isEmpty() ) {
            final TaskStat poll = stats.poll();
            final TaskStat.NodeAndStatWrapper bestStats = poll.getBestStats();
            final Task task = poll.getTask();
            final NodeWithAlloc node = bestStats.getNode();

            final boolean cannotAdd;

            //Check if the node has still enough resources to run the task
            if ( currentlyCopyingTasksOnNode.getOrDefault( node.getNodeLocation(), 0 ) < maxCopyingTaskPerNode
                    &&
                    availableByNodes.get( node ).higherOrEquals( task.getRequest() ) ) {
                if ( createFileAlignment( planedToCopy, nodeTaskAlignments, currentlyCopyingTasksOnNode, poll, task, node, prio ) ) {
                    cannotAdd = false;
                    availableByNodes.get( node ).subFromThis( task.getRequest() );
                } else {
                    cannotAdd = true;
                }
            } else {
                cannotAdd = true;
            }
            //if not enough resources or too many tasks are running, mark next node as to compare and add again into the list
            if ( cannotAdd && poll.increaseIndexToCompare() ) {
                //Only re-add if still other opportunities exist
                stats.add( poll );
            }
        }
        return nodeTaskAlignments;
    }



    private void removeAvailableResources( TaskStats taskStats, Map<NodeWithAlloc, Requirements> availableByNodes, List<NodeWithAlloc> allNodes ) {
        allNodes.parallelStream().forEach( node -> {
            final Requirements availableOnNode = availableByNodes.get( node );
            final Requirements clone = availableOnNode.clone();
            for ( Task task : currentlyCopying.getTasksOnNode( node.getNodeLocation() ) ) {
                final Requirements request = task.getRequest();
                if ( clone.higherOrEquals( request ) ) {
                    if ( availableOnNode.higherOrEquals( request ) ) {
                        //Here we remove the first tasks in the list. However, the ordering of copy tasks finished might be different.
                        availableOnNode.subFromThis( request );
                    }
                    final TaskStat taskStat = taskStats.get( task );
                    //We might still copy data for tasks that have already been started.
                    if ( taskStat != null ) {
                        taskStat.canStartAfterCopying();
                    }
                }
            }
        } );
    }

}
