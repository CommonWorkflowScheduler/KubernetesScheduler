package fonda.scheduler.scheduler.la2;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.util.SortedList;
import fonda.scheduler.util.copying.CurrentlyCopying;
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public abstract class CreateCopyTasks {

    protected final CurrentlyCopying currentlyCopying;
    protected final InputAlignment inputAlignment;
    protected final int copySameTaskInParallel;

    protected FileAlignment getFileAlignmentForTaskAndNode(
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

    /**
     *
     * @return the success of this operation
     */
    protected boolean createFileAlignment(
            CurrentlyCopying planedToCopy,
            List<NodeTaskFilesAlignment> nodeTaskAlignments,
            Map<NodeLocation, Integer> currentlyCopyingTasksOnNode,
            TaskStat poll,
            Task task,
            NodeWithAlloc node,
            int prio
    ) {
        final FileAlignment fileAlignmentForTaskAndNode = getFileAlignmentForTaskAndNode( node, task, poll.getInputsOfTask(), planedToCopy );
        if ( fileAlignmentForTaskAndNode != null && fileAlignmentForTaskAndNode.copyFromSomewhere( node.getNodeLocation() ) ) {
            planedToCopy.addAlignment( fileAlignmentForTaskAndNode.getNodeFileAlignment(), task, node );
            nodeTaskAlignments.add( new NodeTaskFilesAlignment( node, task, fileAlignmentForTaskAndNode, prio ) );
            currentlyCopyingTasksOnNode.compute( node.getNodeLocation(), ( nodeLocation, value ) -> value == null ? 1 : value + 1 );
            poll.copyToNodeWithAvailableResources();
            return true;
        } else {
            return false;
        }
    }

    protected void removeTasksThatAreCopiedMoreThanXTimeCurrently( SortedList<TaskStat> taskStats, int maxParallelTasks ) {
        taskStats.removeIf( elem -> currentlyCopying.getNumberOfNodesForTask( elem.getTask() ) >= maxParallelTasks );
    }

}
