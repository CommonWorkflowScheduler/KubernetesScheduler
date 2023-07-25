package cws.k8s.scheduler.scheduler.la2.copyinadvance;

import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.la2.TaskStat;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;
import cws.k8s.scheduler.util.SortedList;
import cws.k8s.scheduler.util.TaskStats;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class CopyInAdvanceNodeWithMostData extends CopyInAdvance {

    public CopyInAdvanceNodeWithMostData(
            CurrentlyCopying currentlyCopying,
            InputAlignment inputAlignment,
            int copySameTaskInParallel ) {
        super( currentlyCopying, inputAlignment, copySameTaskInParallel );
    }


    /**
     * Do not filter maxHeldCopyTaskReady, that could lead to starving if another node has resources.
     */
    public void createAlignmentForTasksWithEnoughCapacity(
            final List<NodeTaskFilesAlignment> nodeTaskFilesAlignments,
            final TaskStats taskStats,
            final CurrentlyCopying planedToCopy,
            final List<NodeWithAlloc> allNodes,
            final int maxCopyingTaskPerNode,
            final int maxHeldCopyTaskReady,
            final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode,
            int prio,
            Map<NodeWithAlloc, List<Task>> readyTasksPerNode ) {
        final SortedList<TaskStat> stats = new SortedList<>( taskStats.getTaskStats() );
        removeTasksThatAreCopiedMoreThanXTimeCurrently( stats, copySameTaskInParallel );

        while( !stats.isEmpty() ) {
            final TaskStat poll = stats.poll();
            if ( !poll.missingDataOnAnyNode() || poll.isCopyToNodeWithAvailableResources() ) {
                continue;
            }
            long start = System.currentTimeMillis();

            final TaskStat.NodeAndStatWrapper bestStats = poll.getBestStats();
            final Task task = poll.getTask();
            final NodeWithAlloc node = bestStats.getNode();

            final boolean cannotAdd;

            //Check if the node has still enough resources to run the task
            if ( currentlyCopyingTasksOnNode.getOrDefault( node.getNodeLocation(), 0 ) < maxCopyingTaskPerNode ) {
                if ( createFileAlignment( planedToCopy, nodeTaskFilesAlignments, currentlyCopyingTasksOnNode, poll, task, node, prio ) ) {
                    cannotAdd = false;
                    log.info( "Start copy task with {} missing bytes", poll.getBestStats().getTaskNodeStats().getSizeRemaining() );
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
            task.getTraceRecord().addSchedulerTimeDeltaPhaseThree( (int) (System.currentTimeMillis() - start) );
        }
    }

}
