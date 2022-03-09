package fonda.scheduler.util;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealHierarchyFile;

public class FilePath {

    public final String path;
    public final RealHierarchyFile file;
    public final LocationWrapper locationWrapper;

    public FilePath(String path, RealHierarchyFile file, LocationWrapper locationWrapper ) {
        this.path = path;
        this.file = file;
        this.locationWrapper = locationWrapper;
    }
}
