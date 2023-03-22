package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;

public class NodeTaskAlignment {

    public final NodeWithAlloc node;
    public final Task task;

    public NodeTaskAlignment( NodeWithAlloc node, Task task ) {
        this.node = node;
        this.task = task;
    }
}
