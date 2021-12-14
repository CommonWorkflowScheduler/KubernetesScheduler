package fonda.scheduler.rest.response.getfile;

import fonda.scheduler.util.inputs.SymlinkInput;

import java.util.List;

public class FileResponse {

    public final String path;
    public final boolean sameAsEngine;
    public final String node;
    public final String daemon;
    public final List<SymlinkInput> symlinks;
    public final boolean notInContext;

    public FileResponse( String path, String node, String daemon, boolean sameAsEngine, List<SymlinkInput> symlinks) {
        this.path = path;
        this.sameAsEngine = sameAsEngine;
        this.node = node;
        this.daemon = daemon;
        this.symlinks = symlinks;
        notInContext = false;
    }

    public FileResponse( String path, List<SymlinkInput> symlinks) {
        this.path = path;
        this.sameAsEngine = true;
        this.node = null;
        this.daemon = null;
        this.symlinks = symlinks;
        notInContext = true;
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
