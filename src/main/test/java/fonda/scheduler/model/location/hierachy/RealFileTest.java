package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.dag.DAG;
import fonda.scheduler.dag.Process;
import fonda.scheduler.dag.Vertex;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.TaskConfig;
import fonda.scheduler.model.location.NodeLocation;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RealFileTest {

    private Task processA;
    private Task processB;

    @Before
    public void before(){
        DAG dag = new DAG();
        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(new Process("processA", 1));
        vertexList.add(new Process("processB", 2));
        dag.registerVertices(vertexList);
        processA = new Task( new TaskConfig("procesA"), dag);
        processB = new Task( new TaskConfig("procesB"), dag);
    }

    private LocationWrapper getLocationWrapper( String location ){
        return new LocationWrapper( NodeLocation.getLocation(location), 0, 100, processA );
    }


    @Test
    public void addLocation() {

        List<LocationWrapper> locations = new LinkedList<>();


        final LocationWrapper node1 = getLocationWrapper("Node1");
        locations.add(node1);
        final RealFile realFile = new RealFile(node1);
        assertArrayEquals( locations.toArray(), realFile.getLocations() );

        final LocationWrapper node2 = getLocationWrapper("Node2");
        locations.add(node2);
        realFile.addOrUpdateLocation( false, node2);
        assertArrayEquals( locations.toArray(), realFile.getLocations() );

        final LocationWrapper node3 = getLocationWrapper("Node3");
        locations.add(node3);
        realFile.addOrUpdateLocation( false, node3);
        assertArrayEquals( locations.toArray(), realFile.getLocations() );

        final LocationWrapper node1New = getLocationWrapper("Node1");
        realFile.addOrUpdateLocation( false, node1New);
        assertArrayEquals( locations.toArray(), realFile.getLocations());

    }

    @Test
    public void addEmptyLocation() {
        final RealFile realFile = new RealFile( getLocationWrapper("node1") );
        //noinspection ConfusingArgumentToVarargsMethod
        assertThrows(IllegalArgumentException.class, () -> realFile.addOrUpdateLocation( false,  null ));
        assertThrows(IllegalArgumentException.class, () -> realFile.addOrUpdateLocation( false ));
        //noinspection RedundantArrayCreation
        assertThrows(IllegalArgumentException.class, () -> realFile.addOrUpdateLocation( false,  new LocationWrapper[0] ));
        assertThrows(IllegalArgumentException.class, () -> realFile.addOrUpdateLocation( false,  new LocationWrapper[1] ));
    }

    @Test
    public void addInParallel() {
        final LocationWrapper node0 = getLocationWrapper("Node0");
        final RealFile realFile = new RealFile(node0);

        List<LocationWrapper> locations = new LinkedList<>();

        for (int i = 1; i < 10_000; i++) {
            locations.add( getLocationWrapper( "Node" + i ) );
        }

        Collections.shuffle(locations);

        locations.parallelStream().forEach( r -> realFile.addOrUpdateLocation( false, r ) );

        locations.add( node0 );
        assertEquals(
                new HashSet<>(locations),
                new HashSet<>(Arrays.asList(realFile.getLocations()))
        );

    }

    @Test
    public void changeFile() {

        final LocationWrapper node0 = getLocationWrapper("Node0");
        final RealFile realFile = new RealFile(node0);
        final LocationWrapper node1 = getLocationWrapper("Node1");
        realFile.addOrUpdateLocation( false, node1);
        final LocationWrapper node2 = getLocationWrapper("Node2");
        realFile.addOrUpdateLocation( false, node2);
        final LocationWrapper node3 = getLocationWrapper("Node3");
        realFile.addOrUpdateLocation( false, node3);

        final LocationWrapper nodeNew = new LocationWrapper(NodeLocation.getLocation("NodeNew"), 5, 120, processB );
        realFile.addOrUpdateLocation( false,  nodeNew );
        LocationWrapper[] expected = { node0, node1, node2, node3, nodeNew };
        assertArrayEquals( expected, realFile.getLocations() );

    }

    @Test
    public void changeFileOnExistingLocation() {

        final RealFile realFile = new RealFile( getLocationWrapper("Node0") );

        final LocationWrapper nodeNew = new LocationWrapper(NodeLocation.getLocation("Node0"), 5, 120, processA );
        realFile.addOrUpdateLocation( false,  nodeNew );
        LocationWrapper[] expected = { nodeNew };
        assertArrayEquals( expected, realFile.getLocations() );

        final LocationWrapper nodeNew2 = new LocationWrapper(NodeLocation.getLocation("Node0"), 6, 170, processB );
        realFile.addOrUpdateLocation( false,  nodeNew2 );
        LocationWrapper[] expected2 = { nodeNew2 };
        assertArrayEquals( expected2, realFile.getLocations() );

    }

}