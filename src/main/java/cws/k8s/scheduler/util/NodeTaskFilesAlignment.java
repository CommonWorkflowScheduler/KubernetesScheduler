package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.Getter;
import lombok.Setter;

public class NodeTaskFilesAlignment extends NodeTaskAlignment {

    public final FileAlignment fileAlignment;
    public final int prio;

    @Getter
    @Setter
    private boolean removeInit = false;

    public NodeTaskFilesAlignment( NodeWithAlloc node, Task task, FileAlignment fileAlignment, int prio ) {
        super(node, task);
        this.fileAlignment = fileAlignment;
        this.prio = prio;
    }
}
