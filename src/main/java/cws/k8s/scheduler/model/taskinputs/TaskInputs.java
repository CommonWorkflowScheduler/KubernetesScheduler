package cws.k8s.scheduler.model.taskinputs;

import cws.k8s.scheduler.util.TaskNodeStats;
import cws.k8s.scheduler.util.Tuple;
import cws.k8s.scheduler.util.copying.CopySource;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ToString
@Getter
@Slf4j
@RequiredArgsConstructor
public class TaskInputs {

    private final List<SymlinkInput> symlinks;
    private final List<PathFileLocationTriple> files;
    private final Set<Location> excludedNodes;

    private boolean sorted = false;

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

    /**
     * Calculates the data on a node and returns whether all data is on the location
     * @return the size remaining and the amount of data currently copying. Null if the task cannot run on this node.
     */
    public TaskNodeStats calculateMissingData( Location loc, CurrentlyCopyingOnNode currentlyCopying ) {
        long sizeRemaining = 0;
        long sizeCurrentlyCopying = 0;
        long sizeOnNode = 0;
        for ( PathFileLocationTriple fileLocation : files ) {
            final long minSizeInBytes = fileLocation.getMinSizeInBytes();
            //Is the file already on the node?
            if ( fileLocation.locatedOnLocation(loc) ) {
                sizeOnNode += minSizeInBytes;
            } else {
                //is the file currently copying?
                final CopySource copySource = currentlyCopying.getCopySource( fileLocation.path.toString() );
                if ( copySource != null ) {
                    //Is this file compatible with the task?
                    if ( fileLocation.locatedOnLocation( copySource.getLocation() ) ) {
                        sizeCurrentlyCopying += minSizeInBytes;
                    } else {
                        //currently copying file is incompatible with this task
                        return null;
                    }
                } else {
                    sizeRemaining += minSizeInBytes;
                }
            }
        }
        return new TaskNodeStats( sizeRemaining, sizeCurrentlyCopying, sizeOnNode );
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
