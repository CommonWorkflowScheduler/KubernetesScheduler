package fonda.scheduler.model.location.hierachy;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Slf4j
public class HierarchyWrapperTest {

    final String workdir = "/folder/localworkdir/";
    HierarchyWrapper hw;
    List<String> files;
    List<Path> result;
    String temporaryDir = workdir + "/ab/abcdasdasd/test/./abcd/";

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

        files.parallelStream().forEach( x -> assertTrue(hw.addFile(x)));
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
        assertFalse(hw.addFile(temporaryDir + "test/b.txt"));

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    @Test
    public void createFileinWorkdir() {
        assertFalse(hw.addFile(workdir + "ab/b.txt"));

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    @Test
    public void createFileOutOfScope() {
        assertFalse(hw.addFile("/somewhere/test.txt"));
        assertFalse(hw.addFile("/somewhere/on/the/machine/very/deep/hierarchy/test.txt"));

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    @Test
    public void createFileTwice() {
        assertTrue(hw.addFile(temporaryDir + "bc/file.abc"));

        result = hw.getAllFilesInDir(temporaryDir);
        compare( files, result);
    }

    @Test
    public void createFileButWasFolder() {
        assertFalse(hw.addFile(temporaryDir + "bc"));

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

        files.parallelStream().forEach( x -> assertTrue(hw.addFile(x)));

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
}