package cws.k8s.scheduler.rest.response.getfile;

import cws.k8s.scheduler.model.taskinputs.SymlinkInput;
import lombok.ToString;

import java.util.List;

@ToString( exclude = "locationWrapperID" )
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

}
