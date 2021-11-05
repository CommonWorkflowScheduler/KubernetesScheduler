package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RealFileTest {


    @Test
    public void addLocation() {

        final RealFile realFile = new RealFile();
        List<Location> locations = new LinkedList<>();

        locations.add( NodeLocation.getLocation("Node1") );
        realFile.addLocation(NodeLocation.getLocation("Node1"));
        assertArrayEquals( locations.toArray(), realFile.getLocations() );

        locations.add( NodeLocation.getLocation("Node2") );
        realFile.addLocation(NodeLocation.getLocation("Node2"));
        assertArrayEquals( locations.toArray(), realFile.getLocations() );

        locations.add( NodeLocation.getLocation("Node3") );
        realFile.addLocation(NodeLocation.getLocation("Node3"));
        assertArrayEquals( locations.toArray(), realFile.getLocations() );

        realFile.addLocation(NodeLocation.getLocation("Node1"));
        assertArrayEquals( locations.toArray(), realFile.getLocations() );

    }

    @Test
    public void addEmptyLocation() {
        final RealFile realFile = new RealFile();
        assertThrows(IllegalArgumentException.class, () -> realFile.addLocation( null ));
        assertThrows(IllegalArgumentException.class, () -> realFile.addLocation());
        assertThrows(IllegalArgumentException.class, () -> realFile.addLocation( new Location[0] ));
        assertThrows(IllegalArgumentException.class, () -> realFile.addLocation( new Location[1] ));
    }

    @Test
    public void addInParallel() {
        final RealFile realFile = new RealFile();

        List<Location> locations = new LinkedList<>();
        for (int i = 0; i < 10_000; i++) {
            locations.add( NodeLocation.getLocation( "Node" + i ) );
        }

        Collections.shuffle(locations);

        locations.parallelStream().forEach( realFile::addLocation );

        assertEquals(
                new HashSet<>(locations),
                new HashSet<>(Arrays.asList(realFile.getLocations()))
        );

    }

    @Test
    public void changeFile() {

        final RealFile realFile = new RealFile();
        realFile.addLocation( NodeLocation.getLocation("Node1") );
        realFile.addLocation( NodeLocation.getLocation("Node2") );
        realFile.addLocation( NodeLocation.getLocation("Node3") );

        realFile.changeFile( NodeLocation.getLocation("NodeNew") );
        Location[] expected = { NodeLocation.getLocation("NodeNew") };
        assertArrayEquals( expected, realFile.getLocations() );

    }

    @Test
    public void changeFileEmpty() {

        final RealFile realFile = new RealFile();
        assertThrows(IllegalArgumentException.class, () -> realFile.changeFile(null ));
        assertThrows(IllegalArgumentException.class, () -> realFile.changeFile() );
        assertThrows(IllegalArgumentException.class, () -> realFile.changeFile( new Location[0] ));
        assertThrows(IllegalArgumentException.class, () -> realFile.changeFile( new Location[1] ));

    }

}