package cws.k8s.scheduler.model.location.hierachy;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.model.location.NodeLocation;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
class HierarchyWrapperTest {

    final String workdir = "/folder/localworkdir/";
    HierarchyWrapper hw;
    List<Path> files;
    Collection<Path> result;
    final String temporaryDir = workdir + "/ab/abcdasdasd/test/./abcd/";
    LocationWrapper node1;
    DAG dag;

    private LocationWrapper getLocationWrapper( String location ){
        return new LocationWrapper( NodeLocation.getLocation(location), 0, 100, new Task( new TaskConfig("processA"), dag) );
    }

    @BeforeEach
    public void init() {
        dag = new DAG();
        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(new Process("processA", 1));
        vertexList.add(new Process("processB", 2));
        dag.registerVertices(vertexList);

        node1 = getLocationWrapper("Node1");

        hw = new HierarchyWrapper(workdir);
        files = new LinkedList<>();

        files.add( Paths.get(temporaryDir + "test" ));
        files.add( Paths.get(temporaryDir + "file.abc" ));
        files.add( Paths.get(temporaryDir + "a/test.abc" ));
        files.add( Paths.get(temporaryDir + "a/file.abc" ));
        files.add( Paths.get(temporaryDir + "d/test" ));
        files.add( Paths.get(temporaryDir + "d/e/file.txt" ));
        files.add( Paths.get(temporaryDir + "b/c/test.abc" ));
        files.add( Paths.get(temporaryDir + "bc/file.abc" ));

        files.parallelStream().forEach( x -> Assertions.assertNotNull( hw.addFile(x, node1) ) );
        result = hw.getAllFilesInDir(Paths.get(temporaryDir)).keySet();
        compare( files, result);
    }

    private void compare( List<Path> a, Collection<Path> b){
        Assertions.assertEquals( new HashSet<>(a.stream().map( Path::normalize).collect( Collectors.toList())), new HashSet<>(b) );
        Assertions.assertEquals( a.size(), b.size() );
    }


    @Test
    void getAllFilesInDir() {
        log.info("{}", this.result);
        Collection<Path> result;

        result = hw.getAllFilesInDir( Paths.get(temporaryDir + "b/" )).keySet();
        compare(List.of(Paths.get(temporaryDir + "b/c/test.abc")), result);

        log.info("{}", result);

        result = hw.getAllFilesInDir(Paths.get( temporaryDir + "b" )).keySet();
        compare(List.of(Paths.get(temporaryDir + "b/c/test.abc")), result);

        log.info("{}", result);
    }

    @Test
    void getAllFilesInFile() {
        Assertions.assertNull( hw.getAllFilesInDir( Paths.get(temporaryDir + "test/" ) ) );
        Assertions.assertNull( hw.getAllFilesInDir( Paths.get(temporaryDir + "d/test/" ) ) );
        Assertions.assertNull( hw.getAllFilesInDir( Paths.get(temporaryDir + "d/test/c/" ) ) );
    }

    @Test
    void getAllFilesOutOfScrope() {
        Assertions.assertNull( hw.getAllFilesInDir( Paths.get("/somewhere/" ) ) );
        Assertions.assertNull( hw.getAllFilesInDir( Paths.get("/somewhere/on/the/machine/very/deep/hierarchy/" ) ) );
    }

    @Test
    void getAllFilesinAllWorkdirs() {
        Assertions.assertNull( hw.getAllFilesInDir( Paths.get(workdir ) ) );
        Assertions.assertNull( hw.getAllFilesInDir( Paths.get(workdir + "/ab/" ) ) );
    }

    @Test
    void createFileinFile() {
        Assertions.assertNotNull( hw.addFile( Paths.get(temporaryDir + "test/b.txt"), node1) );

        files.remove( Paths.get(temporaryDir + "test" ));
        files.add( Paths.get(temporaryDir + "test/b.txt") );

        result = hw.getAllFilesInDir( Paths.get(temporaryDir) ).keySet();
        compare( files, result);
    }

    @Test
    void createFileinWorkdir() {
        Assertions.assertNull( hw.addFile( Paths.get(workdir + "ab/b.txt"), node1) );

        result = hw.getAllFilesInDir(Paths.get(temporaryDir)).keySet();
        compare( files, result);
    }

    @Test
    void createFileOutOfScope() {
        Assertions.assertNull( hw.addFile( Paths.get("/somewhere/test.txt"), node1) );
        Assertions.assertNull( hw.addFile( Paths.get("/somewhere/on/the/machine/very/deep/hierarchy/test.txt"), node1) );

        result = hw.getAllFilesInDir(Paths.get(temporaryDir)).keySet();
        compare( files, result);
    }

    @Test
    void createFileTwice() {
        Assertions.assertNotNull( hw.addFile( Paths.get(temporaryDir + "bc/file.abc"), node1) );

        result = hw.getAllFilesInDir(Paths.get(temporaryDir)).keySet();
        compare( files, result);
    }

    @Test
    void createFileButWasFolder() {
        Assertions.assertNotNull( hw.addFile( Paths.get(temporaryDir + "bc"),node1) );

        files.remove( Paths.get(temporaryDir + "bc/file.abc") );
        files.add( Paths.get(temporaryDir + "bc") );

        result = hw.getAllFilesInDir(Paths.get(temporaryDir)).keySet();
        compare( files, result);

    }

    @Test
    void testParallelAdd() {
        System.gc();
        long intialMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        List<Path> files = new LinkedList<>();
        String wd = workdir + "ab/abcdefg/";
        Map<String,List<Path>> m = new HashMap<>();

        int iters = 1_000_000;

        for (int i = 0; i < iters; i++) {
            String p = wd + (i % 3) + "/" + (i % 4);
            Path file = Paths.get(p + "/" + "file-" + i);
            files.add( file );
            final List<Path> currentData = m.getOrDefault(p, new LinkedList<>());
            currentData.add(file);
            m.put(p, currentData);
        }

        System.gc();

        long finalMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        log.info( "Memory input: {}mb", (finalMem - intialMem) / 1024 / 1024 );

        intialMem = finalMem;

        files.parallelStream().forEach( x -> Assertions.assertNotNull( hw.addFile(x, getLocationWrapper("Node1")) ) );

        finalMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        log.info( "Memory hierachy: {}mb", (finalMem - intialMem) / 1024 / 1024 );
        System.gc();
        finalMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        log.info( "Memory hierachy after gc: {}mb", (finalMem - intialMem) / 1024 / 1024 );
        log.info( "Memory per entry: {}b", (finalMem - intialMem) / iters );

        result = hw.getAllFilesInDir(  Paths.get( wd ) ).keySet();
        compare(files, result);

        for (Map.Entry<String, List<Path>> entry : m.entrySet()) {
            result = hw.getAllFilesInDir(  Paths.get( entry.getKey() )).keySet();
            compare( entry.getValue(), result);
        }
    }

    @Test
    void testGetFile() {

        hw = new HierarchyWrapper(workdir);
        files = new LinkedList<>();

        files.add( Paths.get( temporaryDir + "test" ) );
        files.add( Paths.get( temporaryDir + "file.abc" ) );
        files.add( Paths.get( temporaryDir + "a/test.abc" ) );
        files.add( Paths.get( temporaryDir + "a/file.abc" ) );
        files.add( Paths.get( temporaryDir + "d/test" ) );
        files.add( Paths.get( temporaryDir + "d/e/file.txt" ) );
        files.add( Paths.get( temporaryDir + "b/c/test.abc" ) );

        int index = 0;
        LocationWrapper[] lw = new LocationWrapper[ files.size() ];
        for ( Path file : files) {
            lw[index] = getLocationWrapper( "Node" + index );
            Assertions.assertNotNull( hw.addFile(file,lw[index++]) );
        }

        result = hw.getAllFilesInDir( Paths.get(temporaryDir) ).keySet();
        compare( files, result);

        index = 0;
        for ( Path file : files) {
            RealHierarchyFile realFile = (RealHierarchyFile) hw.getFile(file);

            final LocationWrapper[] locations = realFile.getLocations();
            Assertions.assertEquals( lw[index++], locations[0] );
            Assertions.assertEquals( 1, locations.length );
        }
    }

    @Test
    void testGetFileOutOfScope() {
        Assertions.assertNull( hw.getFile( Paths.get("/file.txt" ) ) );
        Assertions.assertNull( hw.getFile( Paths.get("/somewhere/on/the/machine/very/deep/hierarchy/file.txt" ) ) );
    }

    @Test
    void testGetFileWorkdir() {
        Assertions.assertNull( hw.getFile( Paths.get(workdir + "ab/test.txt" )) );
    }

    @Test
    void testGetFileButIsDir() {
        final HierarchyFile file = hw.getFile(Paths.get(temporaryDir + "d"));
        Assertions.assertNotNull( file );
        Assertions.assertTrue( file.isDirectory() );
    }

    @Test
    void testFileIsNowDir(){
        Assertions.assertNotNull( hw.addFile( Paths.get(temporaryDir + "d"), getLocationWrapper("nodeXY") ) );
        result = hw.getAllFilesInDir(  Paths.get(temporaryDir ) ).keySet();
        Assertions.assertNotNull( hw.getFile( Paths.get(temporaryDir + "d" )) );
        Assertions.assertNull( hw.getFile( Paths.get(temporaryDir + "d/e/file.txt" )) );
    }
}