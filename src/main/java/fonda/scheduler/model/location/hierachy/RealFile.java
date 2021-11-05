package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.location.Location;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RealFile extends File {

    @Getter
    private Location[] locations = null;

    RealFile(){}

    @Override
    public boolean isDirectory(){
        return false;
    }

    private void checkIfValidInput( Location[] location ){
        if ( location == null || location.length == 0 )
            throw new IllegalArgumentException( "location was null or empty" );
        for (Location loc : location) {
            if ( loc == null ) throw new IllegalArgumentException( "location contains null value" );
        }
    }

    public void addLocation( Location... location ){
        checkIfValidInput( location );
        synchronized ( this ){
            if ( locations == null ){
                locations = location;
                return;
            }
            List<Location> newLocationList = new ArrayList<>( location.length );
            for (Location newLoc : location) {
                boolean foundEqual = false;
                for (Location l : locations) {
                    if ( newLoc.equals(l) ) {
                        foundEqual = true;
                        break;
                    }
                }
                if ( !foundEqual ){
                    newLocationList.add( newLoc );
                }
            }
            final Location[] newLocation = Arrays.copyOf(locations, locations.length + newLocationList.size() );
            int index = locations.length;
            for (Location l : newLocationList) {
                newLocation[index ++ ] = l;
            }
            locations = newLocation;
        }
    }

    public void changeFile( Location... location ){
        checkIfValidInput( location );
        synchronized ( this ) {
            locations = location;
        }
    }

}
