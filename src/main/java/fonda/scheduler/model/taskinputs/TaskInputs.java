package fonda.scheduler.model.taskinputs;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.util.Tuple;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ToString
@Getter
@Slf4j
public class TaskInputs {

    private final List<SymlinkInput> symlinks;
    private final List<PathFileLocationTriple> files;
    private final Set<Location> excludedNodes;

    private boolean sorted = false;

    public TaskInputs(List<SymlinkInput> symlinks, List<PathFileLocationTriple> files, Set<Location> excludedNodes) {
        this.symlinks = symlinks;
        this.files = files;
        this.excludedNodes = excludedNodes;
    }

    public boolean allFilesAreOnLocationAndNotOverwritten( Location loc, Set<String> pathCurrentlyCopying ){
        for (PathFileLocationTriple file : files) {
            if ( !file.locatedOnLocation(loc) || (pathCurrentlyCopying != null && pathCurrentlyCopying.contains(file.path.toString())) ) {
                return false;
            }
        }
        return true;
    }

    public List<LocationWrapper> allLocationWrapperOnLocation( Location loc){
        return files
                .parallelStream()
                .map( file -> file.locationWrapperOnLocation( loc ) )
                .collect( Collectors.toList() );
    }

    public long calculateDataOnNode( Location loc ) {
        return calculateDataOnNodeAdditionalInfo( loc ).getB();
    }

    /**
     * Calculates the data on a node and returns whether all data is on the location
     * @param loc
     * @return boolean: true if all files are on location, Long: data on location
     */
    public Tuple<Boolean,Long> calculateDataOnNodeAdditionalInfo( Location loc ) {
        long size = 0;
        boolean allOnNode = true;
        for ( PathFileLocationTriple fileLocation : files ) {
            if (fileLocation.locatedOnLocation(loc)) {
                size += fileLocation.getSizeInBytes();
            } else {
                allOnNode = false;
            }
        }
        return new Tuple<>( allOnNode, size );
    }

    public long calculateAvgSize() {
        long size = 0;
        for ( PathFileLocationTriple file : files ) {
            size += file.getSizeInBytes();
        }
        return size;
    }

    public void sort(){
        synchronized ( files ) {
            if (!sorted) {
                files.sort((x, y) -> Long.compare(y.getSizeInBytes(), x.getSizeInBytes()));
                sorted = true;
            }
        }
    }

}
