package fonda.scheduler.model.taskinputs;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealHierarchyFile;

import java.nio.file.Path;
import java.util.List;

public class PathFileLocationTriple implements Input {

    public final Path path;
    public final RealHierarchyFile file;
    public final List<LocationWrapper> locations;

    public PathFileLocationTriple(Path path, RealHierarchyFile file, List<LocationWrapper> locations) {
        this.path = path;
        this.file = file;
        this.locations = locations;
    }
}
