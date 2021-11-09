package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Slf4j
public class HierarchyWrapperTest {

    final String workdir = "/folder/localworkdir/";
    HierarchyWrapper hw;
    List<String> files;
    List<Path> result;
    String temporaryDir = workdir + "/ab/abcdasdasd/test/./abcd/";
    Location node1 = NodeLocation.getLocation("Node1");

    @Before
    public void init() {
        hw = new HierarchyWrapper(workdir);
        files = new LinkedList();

        files.add( temporaryDir + "test" );
        files.add( temporaryDir + "file.abc" );
        files.add( temporaryDir + "a/test.abc" );
        files.add( temporaryDir + "a/file.abc" );
        files.add( temporaryDir + "d/test" );
        files.add( temporaryDir + "d/e/file.txt" );
        files.add( temporaryDir + "b/c/test.abc" );
        files.add( temporaryDir + "bc/file.abc" );

        files.parallelStream().forEach( x -> assertTrue(hw.addFile(x,0, node1)));
        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    private void compare( List<String> a, List<Path> b){
        assertEquals(
                new HashSet<>(a.stream().map(x -> Paths.get(x).normalize()).collect(Collectors.toList())),
                new HashSet<>(b)
        );
        assertEquals(a.size(),b.size());
    }


    @Test
    public void getAllFilesInDir() {
        log.info("{}", this.result);
        List<Path> result;

        result = hw.getAllFilesInDir( temporaryDir + "b/" );
        compare(Arrays.asList(temporaryDir + "b/c/test.abc"), result);

        log.info("{}", result);

        result = hw.getAllFilesInDir( temporaryDir + "b" );
        compare(Arrays.asList(temporaryDir + "b/c/test.abc"), result);

        log.info("{}", result);
    }

    @Test
    public void getAllFilesInFile() {
        assertNull(hw.getAllFilesInDir( temporaryDir + "test/" ));
        assertNull(hw.getAllFilesInDir( temporaryDir + "d/test/" ));
        assertNull(hw.getAllFilesInDir( temporaryDir + "d/test/c/" ));
    }

    @Test
    public void getAllFilesOutOfScrope() {
        assertNull(hw.getAllFilesInDir( "/somewhere/" ));
        assertNull(hw.getAllFilesInDir( "/somewhere/on/the/machine/very/deep/hierarchy/" ));
    }

    @Test
    public void getAllFilesinAllWorkdirs() {
        assertNull(hw.getAllFilesInDir( workdir ));
        assertNull(hw.getAllFilesInDir( workdir + "/ab/" ));
    }

    @Test
    public void createFileinFile() {
        assertTrue(hw.addFile(temporaryDir + "test/b.txt", 10, node1));

        files.remove( temporaryDir + "test" );
        files.add( temporaryDir + "test/b.txt" );

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    @Test
    public void createFileinWorkdir() {
        assertFalse(hw.addFile(workdir + "ab/b.txt", 10, node1));

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    @Test
    public void createFileOutOfScope() {
        assertFalse(hw.addFile("/somewhere/test.txt", 10, node1));
        assertFalse(hw.addFile("/somewhere/on/the/machine/very/deep/hierarchy/test.txt", 10, node1));

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    @Test
    public void createFileTwice() {
        assertTrue(hw.addFile(temporaryDir + "bc/file.abc", 10, node1));

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    @Test
    public void createFileButWasFolder() {
        assertTrue(hw.addFile(temporaryDir + "bc", 10, node1));

        files.remove( temporaryDir + "bc/file.abc" );
        files.add( temporaryDir + "bc" );

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);

    }

    @Test
    public void testParallelAdd() {
        System.gc();
        long intialMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        List<String> files = new LinkedList();
        String wd = workdir + "ab/abcdefg/";
        Map<String,List<String>> m = new HashMap();

        int iters = 1_000_000;

        for (int i = 0; i < iters; i++) {
            String p = wd + (i % 3) + "/" + (i % 4);
            String file = p + "/" + "file-" + i;
            files.add( file );
            final List currentData = m.getOrDefault(p, new LinkedList());
            currentData.add(file);
            m.put(p, currentData);
        }

        System.gc();

        long finalMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        log.info( "Memory input: {}mb", (finalMem - intialMem) / 1024 / 1024 );

        intialMem = finalMem;

        files.parallelStream().forEach( x -> assertTrue(hw.addFile(x, 10, node1)));

        finalMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        log.info( "Memory hierachy: {}mb", (finalMem - intialMem) / 1024 / 1024 );
        System.gc();
        finalMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        log.info( "Memory hierachy after gc: {}mb", (finalMem - intialMem) / 1024 / 1024 );
        log.info( "Memory per entry: {}b", (finalMem - intialMem) / iters );

        result = hw.getAllFilesInDir( wd );
        compare(files, result);

        for (Map.Entry<String, List<String>> entry : m.entrySet()) {
            result = hw.getAllFilesInDir( entry.getKey() );
            compare( entry.getValue(), result);
        }
    }

    @Test
    public void testGetFile() {

        hw = new HierarchyWrapper(workdir);
        files = new LinkedList();

        files.add( temporaryDir + "test" );
        files.add( temporaryDir + "file.abc" );
        files.add( temporaryDir + "a/test.abc" );
        files.add( temporaryDir + "a/file.abc" );
        files.add( temporaryDir + "d/test" );
        files.add( temporaryDir + "d/e/file.txt" );
        files.add( temporaryDir + "b/c/test.abc" );

        int index = 0;
        for (String file : files) {
            assertTrue(hw.addFile(file,10, NodeLocation.getLocation( "Node" + index )));
        }

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);

        index = 0;
        for (String file : files) {
            RealFile realFile = hw.getFile(file);

            final Location[] locations = realFile.getLocations();
            assertEquals(NodeLocation.getLocation( "Node" + index ), locations[0]);
            assertEquals(1,locations.length);
        }
    }

    @Test
    public void testGetFileOutOfScope() {
        assertNull(hw.getFile( "/file.txt" ));
        assertNull(hw.getFile( "/somewhere/on/the/machine/very/deep/hierarchy/file.txt" ));
    }

    @Test
    public void testGetFileWorkdir() {
        assertNull(hw.getFile( workdir + "ab/test.txt" ));
    }

    @Test
    public void testGetFileButIsDir() {
        assertNull(hw.getFile( temporaryDir + "d" ));
    }

    @Test
    public void testFileIsNowDir(){
        assertTrue(hw.addFile( temporaryDir + "d", 120, NodeLocation.getLocation("nodeXY") ));
        result = hw.getAllFilesInDir( temporaryDir );
        assertNotNull( hw.getFile( temporaryDir + "d" ) );
        assertNull( hw.getFile( temporaryDir + "d/e/file.txt" ) );
    }
}