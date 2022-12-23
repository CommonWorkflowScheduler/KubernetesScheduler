package fonda.scheduler.model.taskinputs;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealHierarchyFile;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.nio.file.Path;
import java.util.List;

@ToString( exclude = "file" )
@EqualsAndHashCode
public class PathFileLocationTriple implements Input {

    public final Path path;
    public final RealHierarchyFile file;
    public final List<LocationWrapper> locations;
    private long size = -1;

    public PathFileLocationTriple(Path path, RealHierarchyFile file, List<LocationWrapper> locations) {
        this.path = path;
        this.file = file;
        this.locations = locations;
    }

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
