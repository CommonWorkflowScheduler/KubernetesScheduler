package fonda.scheduler.model.taskinputs;

import fonda.scheduler.model.location.NodeLocation;
import lombok.Getter;

import java.util.List;
import java.util.Set;

@Getter
public class TaskInputs {

    private final List<SymlinkInput> symliks;
    private final List<PathFileLocationTriple> files;
    private final Set<NodeLocation> excludedNodes;

    public TaskInputs(List<SymlinkInput> symliks, List<PathFileLocationTriple> files, Set<NodeLocation> excludedNodes) {
        this.symliks = symliks;
        this.files = files;
        this.excludedNodes = excludedNodes;
    }
}
