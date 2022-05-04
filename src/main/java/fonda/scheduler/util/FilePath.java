package fonda.scheduler.util;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealHierarchyFile;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;

public class FilePath {

    private final PathFileLocationTriple pathFileLocationTriple;
    private final LocationWrapper locationWrapper;

    public FilePath( PathFileLocationTriple pathFileLocationTriple, LocationWrapper locationWrapper ) {
        this.pathFileLocationTriple = pathFileLocationTriple;
        this.locationWrapper = locationWrapper;
    }

    public String getPath() {
        return pathFileLocationTriple.path.toString();
    }

    public RealHierarchyFile getFile() {
        return pathFileLocationTriple.file;
    }

    public LocationWrapper getLocationWrapper() {
        return locationWrapper;
    }
}
