package cws.k8s.scheduler.model.location.hierachy;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.InputEdge;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.model.location.LocationType;
import cws.k8s.scheduler.model.location.NodeLocation;
import org.apache.commons.collections4.iterators.PermutationIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RealHierarchyFileTest {

    private Task processA;
    private Task processB;
    private Task processC;
    private Task processD;
    private Task processE;

    @BeforeEach
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
        edgeList.add( new InputEdge(1,1,2) );
        edgeList.add( new InputEdge(2,2,4) );
        edgeList.add( new InputEdge(3,4,5) );
        edgeList.add( new InputEdge(4,1,3) );
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
    void addLocation() {

        List<LocationWrapper> locations = new LinkedList<>();


        final LocationWrapper node1 = getLocationWrapper("Node1");
        locations.add(node1);
        final RealHierarchyFile realFile = new RealHierarchyFile(node1);
        Assertions.assertArrayEquals( locations.toArray(), realFile.getLocations() );

        final LocationWrapper node2 = getLocationWrapper("Node2");
        locations.add(node2);
        realFile.addOrUpdateLocation( false, node2);
        Assertions.assertArrayEquals( locations.toArray(), realFile.getLocations() );

        final LocationWrapper node3 = getLocationWrapper("Node3");
        locations.add(node3);
        realFile.addOrUpdateLocation( false, node3);
        Assertions.assertArrayEquals( locations.toArray(), realFile.getLocations() );

        final LocationWrapper node1New = getLocationWrapper("Node1");
        realFile.addOrUpdateLocation( false, node1New);
        Assertions.assertArrayEquals( locations.toArray(), realFile.getLocations() );

    }

    @Test
    void addEmptyLocation() {
        final RealHierarchyFile realFile = new RealHierarchyFile( getLocationWrapper("node1") );
        assertThrows(IllegalArgumentException.class, () -> realFile.addOrUpdateLocation( false,  null ));
        assertThrows(IllegalArgumentException.class, () -> realFile.addOrUpdateLocation( true,  null ));
    }

    @Test
    void addInParallel() {
        final LocationWrapper node0 = getLocationWrapper("Node0");
        final RealHierarchyFile realFile = new RealHierarchyFile(node0);

        List<LocationWrapper> locations = new LinkedList<>();

        for (int i = 1; i < 10_000; i++) {
            locations.add( getLocationWrapper( "Node" + i ) );
        }

        Collections.shuffle(locations);

        locations.parallelStream().forEach( r -> realFile.addOrUpdateLocation( false, r ) );

        locations.add( node0 );
        Assertions.assertEquals( new HashSet<>(locations), new HashSet<>( Arrays.asList(realFile.getLocations())) );

    }

    @Test
    void changeFile() {

        final LocationWrapper node0 = getLocationWrapper("Node0");
        final RealHierarchyFile realFile = new RealHierarchyFile(node0);
        final LocationWrapper node1 = getLocationWrapper("Node1");
        realFile.addOrUpdateLocation( false, node1);
        final LocationWrapper node2 = getLocationWrapper("Node2");
        realFile.addOrUpdateLocation( false, node2);
        final LocationWrapper node3 = getLocationWrapper("Node3");
        realFile.addOrUpdateLocation( false, node3);

        final LocationWrapper nodeNew = new LocationWrapper(NodeLocation.getLocation("NodeNew"), 5, 120, processB );
        realFile.addOrUpdateLocation( false,  nodeNew );
        LocationWrapper[] expected = { node0, node1, node2, node3, nodeNew };
        Assertions.assertArrayEquals( expected, realFile.getLocations() );

    }

    @Test
    void overwriteFile() {
        final LinkedList<LocationWrapper> results = new LinkedList<>();
        final LocationWrapper node0 = getLocationWrapper("Node0");
        results.add( node0 );
        Assertions.assertTrue( node0.isActive() );
        final RealHierarchyFile realFile = new RealHierarchyFile(node0);
        Assertions.assertArrayEquals( results.toArray(), realFile.getLocations() );
        Assertions.assertTrue( node0.isActive() );
        final LocationWrapper node1 = getLocationWrapper("Node1");
        results.add( node1 );
        realFile.addOrUpdateLocation( true, node1);
        Assertions.assertArrayEquals( results.toArray(), realFile.getLocations() );
        Assertions.assertFalse( node0.isActive() );
        Assertions.assertTrue( node1.isActive() );
        final LocationWrapper node2 = getLocationWrapper("Node2");
        Assertions.assertArrayEquals( results.toArray(), realFile.getLocations() );
        results.add( node2 );
        realFile.addOrUpdateLocation( true, node2);
        Assertions.assertFalse( node0.isActive() );
        Assertions.assertFalse( node1.isActive() );
        Assertions.assertTrue( node2.isActive() );
    }

    @Test
    void changeFileOnExistingLocation() {

        final RealHierarchyFile realFile = new RealHierarchyFile( getLocationWrapper("Node0") );

        final LocationWrapper nodeNew = new LocationWrapper(NodeLocation.getLocation("Node0"), 5, 120, processA );
        realFile.addOrUpdateLocation( false,  nodeNew );
        LocationWrapper[] expected = { nodeNew };
        Assertions.assertArrayEquals( expected, realFile.getLocations() );

        final LocationWrapper nodeNew2 = new LocationWrapper(NodeLocation.getLocation("Node0"), 6, 170, processB );
        realFile.addOrUpdateLocation( false,  nodeNew2 );
        LocationWrapper[] expected2 = { nodeNew2 };
        Assertions.assertArrayEquals( expected2, realFile.getLocations() );

    }

    @Test
    void isFile() {
        final RealHierarchyFile realFile = new RealHierarchyFile( getLocationWrapper("Node0") );
        Assertions.assertFalse( realFile.isDirectory() );
        Assertions.assertFalse( realFile.isSymlink() );
    }


    @Test
    void getFilesForTaskTest() throws InterruptedException, NoAlignmentFoundException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis() - 2, 2, processA );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis() - 1, 2, processB );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis() - 5, 2, processB );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node4"), System.currentTimeMillis() - 2, 2, processC );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node5"), System.currentTimeMillis() - 5, 2, null);

        List<LocationWrapper> locationWrapperList = List.of(loc1, loc2, loc3, loc4);

        PermutationIterator<LocationWrapper> permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealHierarchyFile realFile = new RealHierarchyFile(next.get(0));
            realFile.addOrUpdateLocation(false, next.get(1));
            realFile.addOrUpdateLocation(false, next.get(2));
            realFile.addOrUpdateLocation(false, next.get(3));

            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc1, loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processA).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc1, loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processB).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc1, loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processC).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processD).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc2, loc3, loc4)), new HashSet<>(realFile.getFilesForTask(processE).getMatchingLocations()) );
        }

        locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealHierarchyFile realFile = new RealHierarchyFile(next.get(0));
            realFile.addOrUpdateLocation(false, next.get(1));
            realFile.addOrUpdateLocation(false, next.get(2));
            realFile.addOrUpdateLocation(false, next.get(3));
            realFile.addOrUpdateLocation(false, next.get(4));

            Assertions.assertEquals( new HashSet<>( List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processA).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processB).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processC).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processD).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( List.of(loc5)), new HashSet<>(realFile.getFilesForTask(processE).getMatchingLocations()) );
        }
    }

    @Test
    void getFilesForTaskTestInitFiles() throws InterruptedException, NoAlignmentFoundException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis() - 2, 2, processA );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis() - 1, 2, processB );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node5"), System.currentTimeMillis() - 5, 2, null);
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis() - 5, 2, processB );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node4"), System.currentTimeMillis() - 2, 2, processC );

        List<LocationWrapper> locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        PermutationIterator<LocationWrapper> permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealHierarchyFile realFile = new RealHierarchyFile(next.get(0));
            realFile.addOrUpdateLocation(false, next.get(1));
            realFile.addOrUpdateLocation(false, next.get(2));
            realFile.addOrUpdateLocation(false, next.get(3));
            realFile.addOrUpdateLocation(false, next.get(4));

            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processA).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processB).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processC).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processD).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processE).getMatchingLocations()) );
        }

    }

    @Test
    void getFilesForTaskTestMultipleInitFiles() throws InterruptedException, NoAlignmentFoundException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis() - 2, 2, processA );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis() - 1, 2, processB );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node5"), System.currentTimeMillis() - 5, 2, null);
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis() - 5, 2, processB );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node4"), System.currentTimeMillis() - 2, 2, processC );


        List<LocationWrapper> locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        PermutationIterator<LocationWrapper> permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealHierarchyFile realFile = new RealHierarchyFile(next.get(0));
            realFile.addOrUpdateLocation(false, next.get(1));
            realFile.addOrUpdateLocation(false, next.get(2));
            realFile.addOrUpdateLocation(false, next.get(3));
            realFile.addOrUpdateLocation(false, next.get(4));

            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processA).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processB).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processC).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processD).getMatchingLocations()) );
            Assertions.assertEquals( new HashSet<>( Arrays.asList(loc3, loc4, loc5)), new HashSet<>(realFile.getFilesForTask(processE).getMatchingLocations()) );
        }
    }

    @Test
    void getFilesForTaskTestDifferentAncestors() throws InterruptedException, NoAlignmentFoundException {

        final CountDownLatch waiter = new CountDownLatch(1);

        final NodeLocation location1 = NodeLocation.getLocation("Node1");
        LocationWrapper loc1 = new LocationWrapper( location1, System.currentTimeMillis() - 2, 2, null );
        loc1.use();
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis() - 1, 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        final NodeLocation location3 = NodeLocation.getLocation("Node3");
        LocationWrapper loc3 = new LocationWrapper( location3, System.currentTimeMillis() - 5, 2, null );
        loc3.use();
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        final NodeLocation location4 = NodeLocation.getLocation("Node4");
        LocationWrapper loc4 = new LocationWrapper( location4, System.currentTimeMillis() - 2, 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        final NodeLocation location5 = NodeLocation.getLocation("Node5");
        LocationWrapper loc5 = new LocationWrapper( location5, System.currentTimeMillis() - 5, 2, null );
        loc5.use();

        List<LocationWrapper> locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        PermutationIterator<LocationWrapper> permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealHierarchyFile realFile = new RealHierarchyFile( next.get(0) );
            realFile.addOrUpdateLocation( false, next.get(1) );
            realFile.addOrUpdateLocation( false, next.get(2) );
            realFile.addOrUpdateLocation( false, next.get(3) );
            realFile.addOrUpdateLocation( false, next.get(4) );

            for (Task task : List.of(processA, processB, processC, processD, processE)) {
                final RealHierarchyFile.MatchingLocationsPair filesForTask = realFile.getFilesForTask( task );
                Assertions.assertEquals( new HashSet<>( List.of( loc5 ) ), new HashSet<>( filesForTask.getMatchingLocations() ) );
                Assertions.assertEquals( new HashSet<>( List.of( location1, location3 ) ), new HashSet<>( filesForTask.getExcludedNodes() ) );
            }

        }


        while( loc4.getCreateTime() != loc5.getCreateTime() ){
            loc4 = new LocationWrapper( location4, System.currentTimeMillis() - 2, 2, null );
            loc5 = new LocationWrapper( location5, System.currentTimeMillis() - 5, 2, null );
        }

        locationWrapperList = List.of(loc1, loc2, loc3, loc4, loc5);

        permutationIterator = new PermutationIterator<>(locationWrapperList);
        while ( permutationIterator.hasNext() ) {
            final List<LocationWrapper> next = permutationIterator.next();
            RealHierarchyFile realFile = new RealHierarchyFile( next.get(0) );
            realFile.addOrUpdateLocation( false, next.get(1) );
            realFile.addOrUpdateLocation( false, next.get(2) );
            realFile.addOrUpdateLocation( false, next.get(3) );
            realFile.addOrUpdateLocation( false, next.get(4) );

            for (Task task : List.of(processA, processB, processC, processD, processE)) {
                final RealHierarchyFile.MatchingLocationsPair filesForTask = realFile.getFilesForTask( task );
                Assertions.assertEquals( new HashSet<>( List.of( loc4, loc5 ) ), new HashSet<>( filesForTask.getMatchingLocations() ) );
                Assertions.assertEquals( new HashSet<>( List.of( location1, location3 ) ), new HashSet<>( filesForTask.getExcludedNodes() ) );
            }

        }

    }

    @Test
    void getLastLocationTest() throws InterruptedException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis(), 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis(), 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );

        RealHierarchyFile realFile = new RealHierarchyFile( loc1 );
        Assertions.assertEquals( loc1, realFile.getLastUpdate( LocationType.NODE ) );
        realFile.addOrUpdateLocation(false, loc4 );
        Assertions.assertEquals( loc4, realFile.getLastUpdate( LocationType.NODE ) );
        realFile.addOrUpdateLocation(false, loc2 );
        Assertions.assertEquals( loc4, realFile.getLastUpdate( LocationType.NODE ) );
        realFile.addOrUpdateLocation(false, loc3 );
        Assertions.assertEquals( loc4, realFile.getLastUpdate( LocationType.NODE ) );
        realFile.addOrUpdateLocation(false, loc5 );
        Assertions.assertEquals( loc5, realFile.getLastUpdate( LocationType.NODE ) );

        realFile.addOrUpdateLocation(true, loc2 );
        Assertions.assertEquals( loc2, realFile.getLastUpdate( LocationType.NODE ) );

    }

    @Test
    void getLocationWrapperForNodeTest() throws InterruptedException {

        final CountDownLatch waiter = new CountDownLatch(1);

        LocationWrapper loc1 = new LocationWrapper( NodeLocation.getLocation("Node1"), System.currentTimeMillis(), 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc2 = new LocationWrapper( NodeLocation.getLocation("Node2"), System.currentTimeMillis(), 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc3 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc4 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );
        Assertions.assertFalse( waiter.await(2, TimeUnit.MILLISECONDS ) );
        LocationWrapper loc5 = new LocationWrapper( NodeLocation.getLocation("Node3"), System.currentTimeMillis(), 2, null );

        RealHierarchyFile realFile = new RealHierarchyFile(loc1);
        Assertions.assertEquals( loc1, realFile.getLocationWrapper( NodeLocation.getLocation("Node1") ) );
        realFile.addOrUpdateLocation(false, loc4 );
        Assertions.assertEquals( loc4, realFile.getLocationWrapper( NodeLocation.getLocation("Node3") ) );
        realFile.addOrUpdateLocation(false, loc3 );
        Assertions.assertEquals( loc4, realFile.getLocationWrapper( NodeLocation.getLocation("Node3") ) );
        realFile.addOrUpdateLocation(false, loc2 );
        Assertions.assertEquals( loc2, realFile.getLocationWrapper( NodeLocation.getLocation("Node2") ) );
        realFile.addOrUpdateLocation(false, loc5 );
        Assertions.assertEquals( loc5, realFile.getLocationWrapper( NodeLocation.getLocation("Node3") ) );
        Assertions.assertEquals( loc1, realFile.getLocationWrapper( NodeLocation.getLocation("Node1") ) );

        final NodeLocation node99 = NodeLocation.getLocation("Node99");
        assertThrows( RuntimeException.class, () -> realFile.getLocationWrapper( node99 ) );

    }



}