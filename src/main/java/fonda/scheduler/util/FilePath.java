package fonda.scheduler.util;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealHierarchyFile;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FilePath {

    private final PathFileLocationTriple pathFileLocationTriple;
    private final LocationWrapper locationWrapper;

    public String getPath() {
        return pathFileLocationTriple.path.toString();
    }

    public RealHierarchyFile getFile() {
        return pathFileLocationTriple.file;
    }

    public LocationWrapper getLocationWrapper() {
        return locationWrapper;
    }

    @Override
    public String toString() {
        return getPath();
    }
}
