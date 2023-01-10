package fonda.scheduler.util;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.SymlinkInput;

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
