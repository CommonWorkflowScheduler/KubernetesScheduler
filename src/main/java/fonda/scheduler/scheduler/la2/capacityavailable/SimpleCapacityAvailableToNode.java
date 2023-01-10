package fonda.scheduler.scheduler.la2.capacityavailable;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import fonda.scheduler.scheduler.la2.TaskStat;
import fonda.scheduler.util.*;
import fonda.scheduler.util.copying.CurrentlyCopying;
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class SimpleCapacityAvailableToNode implements CapacityAvailableToNode {

    private final CurrentlyCopying currentlyCopying;
    private final InputAlignment inputAlignment;
    private final int copySameTaskInParallel;

    public List<NodeTaskFilesAlignment> createAlignmentForTasksWithEnoughCapacity(
            final TaskStats taskStats,
            final CurrentlyCopying planedToCopy,
            final Map<NodeWithAlloc, Requirements> availableByNodes,
            final List<NodeWithAlloc> allNodes,
            final int maxCopyingTaskPerNode ) {


        List<NodeTaskFilesAlignment> nodeTaskAlignments = new LinkedList<>();

        final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode = currentlyCopying.getCurrentlyCopyingTasksOnNode();

        //Remove available resources if a copy task is already running: this logic may not be optimal for more than 2 parallel copy tasks (unclear which task starts first)
        removeAvailableResources( taskStats, availableByNodes, allNodes );
        //Sort tasks by missing data: prefer tasks where the least data is missing on the node
        final SortedList<TaskStat> stats = new SortedList<>( taskStats.getTaskStats() );
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
                cannotAdd = !createFileAlignment( planedToCopy, availableByNodes, nodeTaskAlignments, currentlyCopyingTasksOnNode, poll, task, node );
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

    private void removeTasksThatAreCopiedMoreThanXTimeCurrently( SortedList<TaskStat> taskStats, int maxParallelTasks ) {
        taskStats.removeIf( elem -> currentlyCopying.getNumberOfNodesForTask( elem.getTask() ) == maxParallelTasks );
    }


    /**
     *
     * @param planedToCopy
     * @param availableByNodes
     * @param nodeTaskAlignments
     * @param currentlyCopyingTasksOnNode
     * @param poll
     * @param task
     * @param node
     * @return the success of this operation
     */
    private boolean createFileAlignment(
            CurrentlyCopying planedToCopy,
            Map<NodeWithAlloc, Requirements> availableByNodes,
            List<NodeTaskFilesAlignment> nodeTaskAlignments,
            Map<NodeLocation, Integer> currentlyCopyingTasksOnNode,
            TaskStat poll,
            Task task,
            NodeWithAlloc node
    ) {
        final FileAlignment fileAlignmentForTaskAndNode = getFileAlignmentForTaskAndNode( node, task, poll.getInputsOfTask(), planedToCopy );
        if ( fileAlignmentForTaskAndNode != null ) {
            planedToCopy.addAlignment( fileAlignmentForTaskAndNode.getNodeFileAlignment(), task, node );
            nodeTaskAlignments.add( new NodeTaskFilesAlignment( node, task, fileAlignmentForTaskAndNode ) );
            availableByNodes.get( node ).subFromThis( task.getRequest() );
            currentlyCopyingTasksOnNode.compute( node.getNodeLocation(), ( nodeLocation, value ) -> value == null ? 1 : value + 1 );
            return true;
        } else {
            return false;
        }
    }

    private FileAlignment getFileAlignmentForTaskAndNode(
            final NodeWithAlloc node,
            final Task task,
            final TaskInputs inputsOfTask,
            final CurrentlyCopying planedToCopy
    ) {
        final CurrentlyCopyingOnNode currentlyCopyingOnNode = this.currentlyCopying.get(node.getNodeLocation());
        final CurrentlyCopyingOnNode currentlyPlanedToCopy = planedToCopy.get(node.getNodeLocation());
        try {
            return inputAlignment.getInputAlignment(
                    task,
                    inputsOfTask,
                    node,
                    currentlyCopyingOnNode,
                    currentlyPlanedToCopy,
                    Double.MAX_VALUE
            );
        } catch ( NoAligmentPossibleException e ){
            return null;
        }
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
