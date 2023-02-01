package fonda.scheduler.scheduler.nodeassign;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.model.Task;
import fonda.scheduler.scheduler.Scheduler;
import fonda.scheduler.util.NodeTaskAlignment;

import java.util.List;
import java.util.Map;

public abstract class NodeAssign {

    Scheduler scheduler;

    public abstract List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode );

    public void registerScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }


}
