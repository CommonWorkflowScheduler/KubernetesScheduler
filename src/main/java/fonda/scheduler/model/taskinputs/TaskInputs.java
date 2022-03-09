package fonda.scheduler.model.taskinputs;

import fonda.scheduler.model.location.Location;
import lombok.Getter;

import java.util.List;
import java.util.Set;

@Getter
public class TaskInputs {

    private final List<SymlinkInput> symliks;
    private final List<PathFileLocationTriple> files;
    private final Set<Location> excludedNodes;

    public TaskInputs(List<SymlinkInput> symliks, List<PathFileLocationTriple> files, Set<Location> excludedNodes) {
        this.symliks = symliks;
        this.files = files;
        this.excludedNodes = excludedNodes;
    }
}
