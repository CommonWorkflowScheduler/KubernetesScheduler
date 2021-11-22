package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.Process;
import fonda.scheduler.model.location.Location;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RealFile extends File {

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

    private void checkIfValidInput( LocationWrapper[] location ){
        if ( location == null || location.length == 0 )
            throw new IllegalArgumentException( "location was null or empty" );
        for (LocationWrapper loc : location) {
            if ( loc == null ) throw new IllegalArgumentException( "location contains null value" );
        }
    }

    public void addOrUpdateLocation( LocationWrapper... location ){
        checkIfValidInput( location );
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
        return Arrays.asList( locations );
    }

}
