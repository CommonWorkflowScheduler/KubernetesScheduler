package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;

public class NodeTaskFilesAlignment extends NodeTaskAlignment {

    public final FileAlignment fileAlignment;
    public final int prio;
    // Phase in which this alignment was created
    public final int phase;

    public NodeTaskFilesAlignment( NodeWithAlloc node, Task task, FileAlignment fileAlignment, int prio, int phase ) {
        super(node, task);
        this.fileAlignment = fileAlignment;
        this.prio = prio;
        this.phase = phase;
    }
}
