package fonda.scheduler.util;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import lombok.Getter;
import lombok.Setter;

public class NodeTaskFilesAlignment extends NodeTaskAlignment {


    public final FileAlignment fileAlignment;

    @Getter
    @Setter
    private boolean removeInit = false;

    public NodeTaskFilesAlignment( NodeWithAlloc node, Task task, FileAlignment fileAlignment ) {
        super(node, task);
        this.fileAlignment = fileAlignment;
    }
}
