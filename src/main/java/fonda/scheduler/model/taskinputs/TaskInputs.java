package fonda.scheduler.model.taskinputs;

import fonda.scheduler.model.location.Location;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Set;

@ToString
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

    public long calculateDataOnNode( Location loc ) {
        long size = 0;
        for ( PathFileLocationTriple fileLocation : files ) {
            if (fileLocation.locatedOnLocation(loc)) {
                size += fileLocation.getSizeInBytes();
            }
        }
        return size;
    }

    public long calculateAvgSize() {
        long size = 0;
        for ( PathFileLocationTriple file : files ) {
            size += file.getSizeInBytes();
        }
        return size;
    }

}
