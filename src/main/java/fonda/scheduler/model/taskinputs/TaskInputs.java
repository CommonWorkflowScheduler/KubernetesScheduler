package fonda.scheduler.model.taskinputs;

import fonda.scheduler.model.location.Location;
import lombok.Getter;

import java.util.List;
import java.util.Set;

@Getter
public class TaskInputs {

    private final List<SymlinkInput> symlinks;
    private final List<PathFileLocationTriple> files;
    private final Set<Location> excludedNodes;

    public TaskInputs(List<SymlinkInput> symlinks, List<PathFileLocationTriple> files, Set<Location> excludedNodes) {
        this.symlinks = symlinks;
        this.files = files;
        this.excludedNodes = excludedNodes;
    }
}
