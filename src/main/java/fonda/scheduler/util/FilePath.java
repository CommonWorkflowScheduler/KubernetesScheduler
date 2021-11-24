package fonda.scheduler.util;

import fonda.scheduler.model.location.hierachy.RealFile;

public class FilePath {

    public final String path;
    public final RealFile file;

    public FilePath(String path, RealFile file) {
        this.path = path;
        this.file = file;
    }
}
