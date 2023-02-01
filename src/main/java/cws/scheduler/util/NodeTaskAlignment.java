package fonda.scheduler.util;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;

public class NodeTaskAlignment {

    public final NodeWithAlloc node;
    public final Task task;

    public NodeTaskAlignment( NodeWithAlloc node, Task task ) {
        this.node = node;
        this.task = task;
    }
}
