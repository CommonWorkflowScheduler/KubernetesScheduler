package cws.k8s.scheduler.scheduler.la2;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import cws.k8s.scheduler.util.FileAlignment;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;
import cws.k8s.scheduler.util.SortedList;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
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
