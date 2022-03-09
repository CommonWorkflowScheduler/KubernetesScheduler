package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.dag.Process;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.LocationType;
import lombok.Getter;

import java.util.*;

public class RealFile extends AbstractFile {

    /**
     * This field contains the newest LocationWrapper of one file for each node.
     */
    @Getter
    private LocationWrapper[] locations;

    public RealFile( LocationWrapper location ) {
        if ( location == null ) throw new IllegalArgumentException( "location is null" );
        this.locations = new LocationWrapper[]{ location };
    }

    @Override
    public boolean isDirectory(){
        return false;
    }

    @Override
    public boolean isSymlink() {
        return false;
    }

    public LocationWrapper addOrUpdateLocation( boolean overwrite, LocationWrapper location ){
        if ( location == null ) throw new IllegalArgumentException( "location is null" );
        synchronized ( this ){
            LocationWrapper locationWrapperToUpdate = null;
            for (int i = 0; i < locations.length; i++) {
                if ( location.getLocation().equals( locations[i].getLocation() ) ) {
                    locationWrapperToUpdate = locations[i];
                    if ( overwrite || location.getTimestamp() > locations[i].getTimestamp() ) {
                        locationWrapperToUpdate.update( location );
                    }
                    if ( !overwrite ) return locationWrapperToUpdate;
                } else if ( overwrite ){
                    locations[i].deactivate();
                }
            }
            if ( overwrite && locationWrapperToUpdate != null ) return locationWrapperToUpdate;
            final LocationWrapper[] newLocation = Arrays.copyOf(locations, locations.length + 1);
            newLocation[ locations.length ] = location;
            locations = newLocation;
        }
        return location;
    }

    private List<LocationWrapper> combineResultsWithInitial (
                            LinkedList<LocationWrapper> current,
                            LinkedList<LocationWrapper> ancestors,
                            LinkedList<LocationWrapper> descendants,
                            LinkedList<LocationWrapper> unrelated,
                            LinkedList<LocationWrapper> initial
    ) {
        LinkedList<LocationWrapper> result;
        long time = 0;
        //Only keep last update
        if ( initial.size() > 1 ) {
            result = new LinkedList<>();
            for (LocationWrapper locationWrapper : initial) {
                if ( locationWrapper.getCreateTime() > time ) {
                    result.clear();
                    result.add( locationWrapper );
                    time = locationWrapper.getCreateTime();
                } else if  ( locationWrapper.getCreateTime() == time ) {
                    result.add( locationWrapper );
                }
            }
        } else {
            time = initial.get(0).getCreateTime();
            result = initial;
        }
        addAllLaterLocationsToResult( current, result, time );
        if( current == null ) addAllLaterLocationsToResult( ancestors, result, time );
        addAllLaterLocationsToResult( unrelated, result, time );
        addAllLaterLocationsToResult( descendants, result, time );
        return result;
    }

    private List<LocationWrapper> combineResultsEmptyInitial (
            LinkedList<LocationWrapper> current,
            LinkedList<LocationWrapper> ancestors,
            LinkedList<LocationWrapper> descendants,
            LinkedList<LocationWrapper> unrelated
    ) {
        LinkedList<LocationWrapper> result = null;
        if ( current != null ) result = current;
        if ( current == null && ancestors != null ) result = ancestors;
        if ( unrelated != null ) {
            if ( result == null ) result = unrelated;
            else result.addAll( unrelated );
        }
        if ( descendants != null ) {
            if ( result == null ) result = descendants;
            else result.addAll( descendants );
        }
        return result;
    }

    private void addToAncestors(LinkedList<LocationWrapper> ancestors, LocationWrapper location, Process locationProcess) {
        //Add location to list if it could be the last version
        final Iterator<LocationWrapper > iterator = ancestors.iterator();
        Set<Process> locationAncestors = null;
        while (iterator.hasNext()) {
            final LocationWrapper next = iterator.next();
            final Process currentProcess = next.getCreatedByTask().getProcess();
            if (locationProcess == currentProcess) {
                break;
            } else {
                if( locationAncestors == null ) locationAncestors = locationProcess.getAncestors();
                if ( locationAncestors.contains(currentProcess) ) {
                    iterator.remove();
                } else if (locationProcess.getDescendants().contains(currentProcess)) {
                    return;
                }
            }
        }
        ancestors.add(location);
    }

    private LinkedList<LocationWrapper> addAndCreateList(LinkedList<LocationWrapper> list, LocationWrapper toAdd ){
        if ( list == null ) list = new LinkedList<>();
        list.add( toAdd );
        return list;
    }

    public MatchingLocationsPair getFilesForTask( Task task ){
        LocationWrapper[] locationsRef = this.locations;

        LinkedList<LocationWrapper> current = null;
        LinkedList<LocationWrapper> ancestors = null;
        LinkedList<LocationWrapper> descendants = null;
        LinkedList<LocationWrapper> unrelated = null;
        LinkedList<LocationWrapper> initial = null;

        final Process taskProcess = task.getProcess();
        final Set<Process> taskAncestors = taskProcess.getAncestors();
        final Set<Process> taskDescendants = taskProcess.getDescendants();

        Set<Location> inUse = null;

        for ( LocationWrapper location : locationsRef ) {

            if( location.isInUse() ) {
                if ( inUse == null ) inUse = new HashSet<>();
                inUse.add(location.getLocation());
            }

            if ( !location.isActive() ) continue;

            //File was modified by an operator (no relation known)
            if ( location.getCreatedByTask() == null ) {
                initial = addAndCreateList( initial, location );
                continue;
            }

            final Process locationProcess = location.getCreatedByTask().getProcess();
            if ( locationProcess == taskProcess)
                //Location was created by the same process == does definitely fit.
                current = addAndCreateList( current, location );
            else if ( taskAncestors.contains(locationProcess) ) {
                // location is a direct ancestor
                if ( ancestors == null ) {
                    ancestors = new LinkedList<>();
                    ancestors.add( location );
                } else {
                    addToAncestors( ancestors, location, locationProcess );
                }
            }
            else if ( taskDescendants.contains(locationProcess) )
                // location is a direct descendant
                descendants = addAndCreateList( descendants, location );
            else
                // location was possibly generated in parallel
                unrelated = addAndCreateList( unrelated, location );
        }

        final List<LocationWrapper> matchingLocations = ( initial == null )
             ? combineResultsEmptyInitial( current, ancestors, descendants, unrelated )
            : combineResultsWithInitial( current, ancestors, descendants, unrelated, initial );

        removeMatchingLocations( matchingLocations, inUse );

        return new MatchingLocationsPair( matchingLocations, inUse );
    }

    private void removeMatchingLocations( List<LocationWrapper> matchingLocations, Set<Location> locations ){
        if ( locations == null ) return;
        for ( LocationWrapper matchingLocation : matchingLocations ) {
            if( matchingLocation.isInUse() ) {
                locations.remove(matchingLocation.getLocation());
            }
        }
    }

    @Getter
    public class MatchingLocationsPair {

        private final List<LocationWrapper> matchingLocations;
        private final Set<Location> excludedNodes;

        private MatchingLocationsPair(List<LocationWrapper> matchingLocations, Set<Location> excludedNodes) {
            this.matchingLocations = matchingLocations;
            this.excludedNodes = excludedNodes;
        }

    }

    private void addAllLaterLocationsToResult( List<LocationWrapper> list, List<LocationWrapper> result, long time ){
        if( list != null ) {
            for ( LocationWrapper l : list ) {
                if( l.getCreateTime() >= time ) result.add( l );
            }
        }
    }

    public LocationWrapper getLastUpdate( LocationType type ){
        LocationWrapper lastLocation = null;
        for (LocationWrapper location : locations) {
            if( location.isActive() && location.getLocation().getType() == type && (lastLocation == null || lastLocation.getCreateTime() < location.getCreateTime() )){
                lastLocation = location;
            }
        }
        return lastLocation;
    }

    public LocationWrapper getLocationWrapper( Location location ){
        for (LocationWrapper locationWrapper : locations) {
            if ( locationWrapper.isActive() && locationWrapper.getLocation() == location ) return locationWrapper;
        }
        throw new RuntimeException( "Not found: " + location.getIdentifier() );
    }

}
