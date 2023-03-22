package cws.k8s.scheduler.scheduler.nodeassign;

import cws.k8s.scheduler.scheduler.Scheduler;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.util.NodeTaskAlignment;

import java.util.List;
import java.util.Map;

public abstract class NodeAssign {

    Scheduler scheduler;

    public abstract List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode );

    public void registerScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }


}
