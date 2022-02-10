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

    public RealFile( LocationWrapper... locations ) {
        checkIfValidInput( locations );
        this.locations = locations;
    }

    @Override
    public boolean isDirectory(){
        return false;
    }

    @Override
    public boolean isSymlink() {
        return false;
    }

    private void checkIfValidInput( LocationWrapper[] location ){
        if ( location == null || location.length == 0 )
            throw new IllegalArgumentException( "location was null or empty" );
        for (LocationWrapper loc : location) {
            if ( loc == null ) throw new IllegalArgumentException( "location contains null value" );
        }
    }

    public void addOrUpdateLocation( boolean overwrite, LocationWrapper... location ){
        checkIfValidInput( location );
        if ( overwrite ){
            this.locations = location;
            return;
        }
        synchronized ( this ){
            int index = 0;
            LocationWrapper[] newLocationsTmp = new LocationWrapper[location.length];
            for (LocationWrapper newLoc : location) {
                boolean foundEqual = false;
                for (int i = 0; i < locations.length; i++) {
                    if ( newLoc.getLocation().equals( locations[i].getLocation() ) ) {
                        foundEqual = true;
                        if ( newLoc.getTimestamp() > locations[i].getTimestamp() )
                            locations[i] = newLoc;
                        break;
                    }
                }
                if ( !foundEqual ){
                    newLocationsTmp[index++] = newLoc;
                }
            }
            final LocationWrapper[] newLocation = Arrays.copyOf(locations, locations.length + index );
            System.arraycopy( newLocationsTmp, 0, newLocation, locations.length, index );
            locations = newLocation;
        }
    }

    public List<LocationWrapper> getFilesForTask( Task task ){
        LocationWrapper[] locations = this.locations;

        LinkedList<LocationWrapper> results = new LinkedList<>();
        LinkedList<LocationWrapper> ancestors = null;
        LinkedList<LocationWrapper> descendants = null;
        LinkedList<LocationWrapper> unrelated = null;
        LinkedList<LocationWrapper> initial = null;

        for (LocationWrapper location : locations) {

            LinkedList<LocationWrapper> listToFilter;
            Set<Process> locationsAncestors;
            Process locationProcess;

            if ( location.getCreatedByTask() == null ) {
                //File was modified by an operator (no relation known)
                if ( initial == null ) initial = new LinkedList<>();
                initial.add( location );
                continue;
            } else if ( (locationProcess = location.getCreatedByTask().getProcess() ) == task.getProcess() ) {
                //Location was created by the same process == does definitely fit.
                results.add( location );
                continue;
            } else if ( (locationsAncestors = locationProcess.getAncestors()).contains( task.getProcess() ) ) {
                // location is a direct ancestor
                if ( ancestors == null ) ancestors = new LinkedList<>();
                listToFilter = ancestors;
            } else if ( locationProcess.getDescendants().contains( task.getProcess() ) ) {
                // location is a direct descendant
                if ( descendants == null ) descendants = new LinkedList<>();
                descendants.add( location );
                continue;
            } else {
                // location was possibly generated in parallel
                if ( unrelated == null ) unrelated = new LinkedList<>();
                listToFilter = unrelated;
            }

            //Add location to list if it could be the last version
            if ( listToFilter.isEmpty() ) listToFilter.add( location );
            else {
                final Iterator<LocationWrapper > iterator = listToFilter.iterator();
                final Set<Process> locationsDescendants = locationProcess.getDescendants();
                boolean isAncestor = false;
                while (iterator.hasNext()) {

                    final LocationWrapper next = iterator.next();
                    final Process currentProcess = next.getCreatedByTask().getProcess();
                    if (locationProcess == currentProcess) {
                        break;
                    } else if (locationsAncestors.contains(currentProcess)) {
                        iterator.remove();
                    } else if (locationsDescendants.contains(currentProcess)) {
                        isAncestor = true;
                        break;
                    }

                }
                if (!isAncestor) listToFilter.add(location);
            }

        }

        if ( ancestors == null && unrelated == null && descendants == null ){
            results.addAll( initial );
        } else {
            if ( ancestors != null ) results.addAll( ancestors );
            if ( unrelated != null ) results.addAll( unrelated );
            if ( descendants != null ) results.addAll( descendants );
        }
        return results;
    }

    public LocationWrapper getLastUpdate( LocationType type ){
        LocationWrapper lastLocation = null;
        for (LocationWrapper location : locations) {
            if( location.getLocation().getType() == type && (lastLocation == null || lastLocation.getCreateTime() < location.getCreateTime() )){
                lastLocation = location;
            }
        }
        return lastLocation;
    }

    public LocationWrapper getLocationWrapper( Location location ){
        for (LocationWrapper locationWrapper : locations) {
            if ( locationWrapper.getLocation() == location ) return locationWrapper;
        }
        throw new RuntimeException( "Not found: " + location.getIdentifier() );
    }

}
