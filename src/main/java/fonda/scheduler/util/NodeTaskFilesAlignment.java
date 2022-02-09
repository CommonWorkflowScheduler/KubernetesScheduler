package fonda.scheduler.util;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;

public class NodeTaskFilesAlignment extends NodeTaskAlignment {


    public final FileAlignment fileAlignment;

    public NodeTaskFilesAlignment( NodeWithAlloc node, Task task, FileAlignment fileAlignment ) {
        super(node, task);
        this.fileAlignment = fileAlignment;
    }
}
