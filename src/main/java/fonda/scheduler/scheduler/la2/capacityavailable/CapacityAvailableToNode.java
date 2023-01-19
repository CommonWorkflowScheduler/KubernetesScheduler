package fonda.scheduler.scheduler.la2.capacityavailable;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.scheduler.la2.CreateCopyTasks;
import fonda.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.util.TaskStats;
import fonda.scheduler.util.copying.CurrentlyCopying;

import java.util.List;
import java.util.Map;

public abstract class CapacityAvailableToNode extends CreateCopyTasks {

    public CapacityAvailableToNode(
            CurrentlyCopying currentlyCopying,
            InputAlignment inputAlignment,
            int copySameTaskInParallel ) {
        super( currentlyCopying, inputAlignment, copySameTaskInParallel );
    }

    public abstract List<NodeTaskFilesAlignment> createAlignmentForTasksWithEnoughCapacity(
            final TaskStats taskStats,
            final CurrentlyCopying planedToCopy,
            final Map<NodeWithAlloc, Requirements> availableByNodes,
            final List<NodeWithAlloc> allNodes,
            final int maxCopyingTaskPerNode,
            final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode,
            int prio
    );

}
