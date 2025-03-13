package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.taskinputs.SymlinkInput;

import java.util.List;

public class NodeTaskLocalFilesAlignment extends NodeTaskAlignment {

    public final List<SymlinkInput> symlinks;
    public final List<LocationWrapper> locationWrappers;

    public NodeTaskLocalFilesAlignment( NodeWithAlloc node, Task task, List<SymlinkInput> symlinks, List<LocationWrapper> locationWrappers ) {
        super(node, task);
        this.symlinks = symlinks;
        this.locationWrappers = locationWrappers;
    }

}
