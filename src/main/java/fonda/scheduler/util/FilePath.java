package fonda.scheduler.util;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealFile;

public class FilePath {

    public final String path;
    public final RealFile file;
    public final LocationWrapper locationWrapper;

    public FilePath(String path, RealFile file, LocationWrapper locationWrapper ) {
        this.path = path;
        this.file = file;
        this.locationWrapper = locationWrapper;
    }
}
