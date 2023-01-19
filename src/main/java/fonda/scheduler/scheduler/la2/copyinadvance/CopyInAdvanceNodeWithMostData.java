package fonda.scheduler.scheduler.la2.copyinadvance;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.scheduler.la2.CreateCopyTasks;
import fonda.scheduler.scheduler.la2.TaskStat;
import fonda.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.util.SortedList;
import fonda.scheduler.util.TaskStats;
import fonda.scheduler.util.copying.CurrentlyCopying;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class CopyInAdvanceNodeWithMostData extends CreateCopyTasks {

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
            int prio
    ) {
        final SortedList<TaskStat> stats = new SortedList<>( taskStats.getTaskStats() );
        removeTasksThatAreCopiedMoreThanXTimeCurrently( stats, copySameTaskInParallel );

        while( !stats.isEmpty() ) {
            final TaskStat poll = stats.poll();

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
        }
    }

}
