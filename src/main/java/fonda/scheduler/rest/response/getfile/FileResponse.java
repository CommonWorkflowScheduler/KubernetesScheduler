package fonda.scheduler.rest.response.getfile;

import fonda.scheduler.model.taskinputs.SymlinkInput;

import java.util.List;

public class FileResponse {

    public final String path;
    public final boolean sameAsEngine;
    public final String node;
    public final String daemon;
    public final List<SymlinkInput> symlinks;
    public final boolean notInContext;
    public final long locationWrapperID;

    public FileResponse( String path, String node, String daemon, boolean sameAsEngine, List<SymlinkInput> symlinks, long locationWrapperID) {
        this.path = path;
        this.sameAsEngine = sameAsEngine;
        this.node = node;
        this.daemon = daemon;
        this.symlinks = symlinks;
        notInContext = false;
        this.locationWrapperID = locationWrapperID;
    }

    public FileResponse( String path, List<SymlinkInput> symlinks) {
        this.path = path;
        this.sameAsEngine = true;
        this.node = null;
        this.daemon = null;
        this.symlinks = symlinks;
        notInContext = true;
        locationWrapperID = -1;
    }

    @Override
    public String toString() {
        return "FileResponse{" +
                "path='" + path + '\'' +
                ", sameAsEngine=" + sameAsEngine +
                ", node='" + node + '\'' +
                ", daemon='" + daemon + '\'' +
                ", symlinks=" + symlinks +
                ", notInContext=" + notInContext +
                '}';
    }
}
