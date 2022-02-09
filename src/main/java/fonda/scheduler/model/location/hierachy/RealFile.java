package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.dag.Process;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.LocationType;
import lombok.Getter;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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

    public List<LocationWrapper> getFilesForProcess( Process process ){
        LocationWrapper lastUpdated = null;
        for (LocationWrapper location : locations) {
            if ( lastUpdated == null || lastUpdated.getCreateTime() < location.getCreateTime() ) {
                lastUpdated = location;
            }
        }
        return lastUpdated == null ? new LinkedList<>() : List.of( lastUpdated );
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
