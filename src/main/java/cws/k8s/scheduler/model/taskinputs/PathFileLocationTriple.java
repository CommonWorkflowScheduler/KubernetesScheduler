package cws.k8s.scheduler.model.taskinputs;

import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.location.hierachy.RealHierarchyFile;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.nio.file.Path;
import java.util.List;

@ToString( exclude = "file" )
@EqualsAndHashCode
@RequiredArgsConstructor
public class PathFileLocationTriple implements Input {

    public final Path path;
    public final RealHierarchyFile file;
    public final List<LocationWrapper> locations;
    private long size = -1;

    public long getSizeInBytes() {
        if ( this.size != -1 ) {
            return this.size;
        }
        long currentSize = 0;
        for (LocationWrapper location : locations) {
            currentSize += location.getSizeInBytes();
        }
        this.size = currentSize / locations.size();
        return this.size;
    }

    public long getMinSizeInBytes() {
        if ( locations.isEmpty() ) {
            throw new IllegalStateException("No locations for file " + path);
        }
        long minSize = Long.MAX_VALUE;
        for (LocationWrapper location : locations) {
            if ( location.getSizeInBytes() < minSize ) {
                minSize = location.getSizeInBytes();
            }
        }
        return minSize;
    }

    public LocationWrapper locationWrapperOnLocation(Location loc){
        for (LocationWrapper location : locations) {
            if ( location.getLocation().equals(loc) ) {
                return location;
            }
        }
        throw new IllegalStateException("LocationWrapper not found for location " + loc);
    }

    public boolean locatedOnLocation(Location loc){
        for (LocationWrapper location : locations) {
            if ( location.getLocation() == loc ) {
                return true;
            }
        }
        return false;
    }

}
