package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.dag.DAG;
import fonda.scheduler.dag.InputEdge;
import fonda.scheduler.dag.Process;
import fonda.scheduler.dag.Vertex;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.TaskConfig;
import fonda.scheduler.model.location.LocationType;
import fonda.scheduler.model.location.NodeLocation;
import org.apache.commons.collections4.iterators.PermutationIterator;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RealFileTest {

    private Task processA;
    private Task processB;
    private Task processC;
    private Task processD;
    private Task processE;

    @Before
    public void before(){
        DAG dag = new DAG();
        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(new Process("processA", 1));
        vertexList.add(new Process("processB", 2));
        vertexList.add(new Process("processC", 3));
        vertexList.add(new Process("processD", 4));
        vertexList.add(new Process("processE", 5));
        dag.registerVertices(vertexList);
        List<InputEdge> edgeList = new LinkedList<>();
        edgeList.add( new InputEdge(1,2) );
        edgeList.add( new InputEdge(2,4) );
        edgeList.add( new InputEdge(4,5) );
        edgeList.add( new InputEdge(1,3) );
        dag.registerEdges(edgeList);
        processA = new Task( new TaskConfig("processA"), dag);
        processB = new Task( new TaskConfig("processB"), dag);
        processC = new Task( new TaskConfig("processC"), dag);
        processD = new Task( new TaskConfig("processD"), dag);
        processE = new Task( new TaskConfig("processE"), dag);
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
        assertThrows(IllegalArgumentException.class, () -> realFile.addOrUpdateLocation( false,  null ));
        assertThrows(IllegalArgumentException.class, () -> realFile.addOrUpdateLocation( true,  null ));
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

    @Test
    public void isFile() {
        final RealFile realFile = new RealFile( getLocationWrapper("Node0") );
        assertFalse( realFile.isDirectory() );
        assertFalse( realFile.isSymlink() );
    }


    @Test
    public void getFilesForTaskTest() throws InterruptedException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis() - 2, 2, processA );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis() - 1, 2, processB );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis() - 5, 2, processB );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node4"), System.currentTimeMillis() - 2, 2, processC );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node5"), System.currentTimeMillis() - 5, 2, null);

        List<LocationWrapper> locationWrapperList = List.of(loc1, loc2, loc3, loc4);

        PermutationIterator<LocationWrapper> permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealFile realFile = new RealFile(next.get(0));
            realFile.addOrUpdateLocation(false, next.get(1));
            realFile.addOrUpdateLocation(false, next.get(2));
            realFile.addOrUpdateLocation(false, next.get(3));

            assertEquals(new HashSet<>(Arrays.asList(loc1, loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processA)));
            assertEquals(new HashSet<>(Arrays.asList(loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processB)));
            assertEquals(new HashSet<>(Arrays.asList(loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processC)));
            assertEquals(new HashSet<>(Arrays.asList(loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processD)));
            assertEquals(new HashSet<>(Arrays.asList(loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processE)));
        }

        locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealFile realFile = new RealFile(next.get(0));
            realFile.addOrUpdateLocation(false, next.get(1));
            realFile.addOrUpdateLocation(false, next.get(2));
            realFile.addOrUpdateLocation(false, next.get(3));
            realFile.addOrUpdateLocation(false, next.get(4));

            assertEquals(new HashSet<>(List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processA)));
            assertEquals(new HashSet<>(List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processB)));
            assertEquals(new HashSet<>(List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processC)));
            assertEquals(new HashSet<>(List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processD)));
            assertEquals(new HashSet<>(List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processE)));
        }
    }

    @Test
    public void getFilesForTaskTestInitFiles() throws InterruptedException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis() - 2, 2, processA );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis() - 1, 2, processB );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node5"), System.currentTimeMillis() - 5, 2, null);
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis() - 5, 2, processB );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node4"), System.currentTimeMillis() - 2, 2, processC );

        List<LocationWrapper> locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        PermutationIterator<LocationWrapper> permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealFile realFile = new RealFile(next.get(0));
            realFile.addOrUpdateLocation(false, next.get(1));
            realFile.addOrUpdateLocation(false, next.get(2));
            realFile.addOrUpdateLocation(false, next.get(3));
            realFile.addOrUpdateLocation(false, next.get(4));

            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processA)));
            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processB)));
            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processC)));
            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processD)));
            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processE)));
        }

    }

    @Test
    public void getFilesForTaskTestMultipleInitFiles() throws InterruptedException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis() - 2, 2, processA );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis() - 1, 2, processB );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node5"), System.currentTimeMillis() - 5, 2, null);
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis() - 5, 2, processB );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node4"), System.currentTimeMillis() - 2, 2, processC );


        List<LocationWrapper> locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        PermutationIterator<LocationWrapper> permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealFile realFile = new RealFile(next.get(0));
            realFile.addOrUpdateLocation(false, next.get(1));
            realFile.addOrUpdateLocation(false, next.get(2));
            realFile.addOrUpdateLocation(false, next.get(3));
            realFile.addOrUpdateLocation(false, next.get(4));

            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processA)));
            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processB)));
            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processC)));
            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processD)));
            assertEquals(new HashSet<>(Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processE)));
        }
    }

    @Test
    public void getFilesForTaskTestDifferentAncestors() throws InterruptedException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis() - 2, 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis() - 1, 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis() - 5, 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node4"), System.currentTimeMillis() - 2, 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node5"), System.currentTimeMillis() - 5, 2, null );

        List<LocationWrapper> locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        PermutationIterator<LocationWrapper> permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealFile realFile = new RealFile( next.get(0) );
            realFile.addOrUpdateLocation( false, next.get(1) );
            realFile.addOrUpdateLocation( false, next.get(2) );
            realFile.addOrUpdateLocation( false, next.get(3) );
            realFile.addOrUpdateLocation( false, next.get(4) );

            assertEquals( new HashSet<>( List.of( loc5 ) ), new HashSet<>( realFile.getFilesForTask( processA ) ) );
            assertEquals( new HashSet<>( List.of( loc5 ) ), new HashSet<>( realFile.getFilesForTask( processB ) ) );
            assertEquals( new HashSet<>( List.of( loc5 ) ), new HashSet<>( realFile.getFilesForTask( processC ) ) );
            assertEquals( new HashSet<>( List.of( loc5 ) ), new HashSet<>( realFile.getFilesForTask( processD ) ) );
            assertEquals( new HashSet<>( List.of( loc5 ) ), new HashSet<>( realFile.getFilesForTask( processE ) ) );
        }


        while( loc4.getCreateTime() != loc5.getCreateTime() ){
            loc4 = new LocationWrapper( NodeLocation.getLocation("Node4"), System.currentTimeMillis() - 2, 2, null );
            loc5 = new LocationWrapper( NodeLocation.getLocation("Node5"), System.currentTimeMillis() - 5, 2, null );
        }

        locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealFile realFile = new RealFile( next.get(0) );
            realFile.addOrUpdateLocation( false, next.get(1) );
            realFile.addOrUpdateLocation( false, next.get(2) );
            realFile.addOrUpdateLocation( false, next.get(3) );
            realFile.addOrUpdateLocation( false, next.get(4) );

            assertEquals( new HashSet<>( Arrays.asList( loc4, loc5 ) ), new HashSet<>( realFile.getFilesForTask( processA ) ) );
            assertEquals( new HashSet<>( Arrays.asList( loc4, loc5 ) ), new HashSet<>( realFile.getFilesForTask( processB ) ) );
            assertEquals( new HashSet<>( Arrays.asList( loc4, loc5 ) ), new HashSet<>( realFile.getFilesForTask( processC ) ) );
            assertEquals( new HashSet<>( Arrays.asList( loc4, loc5 ) ), new HashSet<>( realFile.getFilesForTask( processD ) ) );
            assertEquals( new HashSet<>( Arrays.asList( loc4, loc5 ) ), new HashSet<>( realFile.getFilesForTask( processE ) ) );
        }


    }

    @Test
    public void getLastLocationTest() throws InterruptedException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis(), 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis(), 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );

        RealFile realFile = new RealFile( loc1 );
        assertEquals( loc1, realFile.getLastUpdate( LocationType.NODE ));
        realFile.addOrUpdateLocation(false, loc4 );
        assertEquals( loc4, realFile.getLastUpdate( LocationType.NODE ));
        realFile.addOrUpdateLocation(false, loc2 );
        assertEquals( loc4, realFile.getLastUpdate( LocationType.NODE ));
        realFile.addOrUpdateLocation(false, loc3 );
        assertEquals( loc4, realFile.getLastUpdate( LocationType.NODE ));
        realFile.addOrUpdateLocation(false, loc5 );
        assertEquals( loc5, realFile.getLastUpdate( LocationType.NODE ));

        realFile.addOrUpdateLocation(true, loc2 );
        assertEquals( loc2, realFile.getLastUpdate( LocationType.NODE ));

    }

    @Test
    public void getLocationWrapperForNodeTest() throws InterruptedException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis(), 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis(), 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );
        assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );

        RealFile realFile = new RealFile(loc1);
        assertEquals( loc1, realFile.getLocationWrapper( NodeLocation.getLocation("Node1") ) );
        realFile.addOrUpdateLocation(false, loc4 );
        assertEquals( loc4, realFile.getLocationWrapper( NodeLocation.getLocation("Node3") ) );
        realFile.addOrUpdateLocation(false, loc3 );
        assertEquals( loc4, realFile.getLocationWrapper( NodeLocation.getLocation("Node3") ) );
        realFile.addOrUpdateLocation(false, loc2 );
        assertEquals( loc2, realFile.getLocationWrapper( NodeLocation.getLocation("Node2") ) );
        realFile.addOrUpdateLocation(false, loc5 );
        assertEquals( loc5, realFile.getLocationWrapper( NodeLocation.getLocation("Node3") ) );
        assertEquals( loc1, realFile.getLocationWrapper( NodeLocation.getLocation("Node1") ) );

        final NodeLocation node99 = NodeLocation.getLocation("Node99");
        assertThrows( RuntimeException.class, () -> realFile.getLocationWrapper( node99 ) );

    }



}