package cws.k8s.scheduler.scheduler.la2.copyinadvance;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.la2.CreateCopyTasks;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;
import cws.k8s.scheduler.util.TaskStats;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;

import java.util.List;
import java.util.Map;

public abstract class CopyInAdvance extends CreateCopyTasks {

    public CopyInAdvance( CurrentlyCopying currentlyCopying, InputAlignment inputAlignment, int copySameTaskInParallel ) {
        super( currentlyCopying, inputAlignment, copySameTaskInParallel );
    }

    public abstract void createAlignmentForTasksWithEnoughCapacity(
            final List<NodeTaskFilesAlignment> nodeTaskFilesAlignments,
            final TaskStats taskStats,
            final CurrentlyCopying planedToCopy,
            final List<NodeWithAlloc> allNodes,
            final int maxCopyingTaskPerNode,
            final int maxHeldCopyTaskReady,
            final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode,
            int prio,
            Map<NodeWithAlloc, List<Task>> readyTasksPerNode );

}
