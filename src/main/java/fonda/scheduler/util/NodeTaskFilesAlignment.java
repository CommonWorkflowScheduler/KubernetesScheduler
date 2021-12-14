package fonda.scheduler.util;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;

import java.util.List;
import java.util.Map;

public class NodeTaskFilesAlignment extends NodeTaskAlignment {


    public final Map<String, List<FilePath>> nodeFileAlignment;

    public NodeTaskFilesAlignment( NodeWithAlloc node, Task task, Map<String, List<FilePath>> nodeFileAlignment ) {
        super(node, task);
        this.nodeFileAlignment = nodeFileAlignment;
    }
}
