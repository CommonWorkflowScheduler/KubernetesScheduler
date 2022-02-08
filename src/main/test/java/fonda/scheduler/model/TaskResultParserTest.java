package fonda.scheduler.model;

import fonda.scheduler.dag.DAG;
import fonda.scheduler.dag.Process;
import fonda.scheduler.dag.Vertex;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.outfiles.OutputFile;
import fonda.scheduler.model.outfiles.PathLocationWrapperPair;
import fonda.scheduler.model.outfiles.SymlinkOutput;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.Assert.*;

@Slf4j
public class TaskResultParserTest {

    private final String NL = "\n";

    private Path storeData( String[] inputdata, String[] outputdata ){
        Path tmpDir = null;

        try {

            tmpDir = Files.createTempDirectory( "results" );
            log.info("Path: {}", tmpDir );
            PrintWriter pw = new PrintWriter( tmpDir.resolve(".command.infiles").toString() );
            for (String s : inputdata) {
                pw.println( s );
            }
            pw.close();

            pw = new PrintWriter( tmpDir.resolve(".command.outfiles").toString() );
            for (String s : outputdata) {
                pw.println( s );
            }
            pw.close();
        } catch ( Exception e ){}
        return tmpDir;
    }

    private DAG dag;

    @Before
    public void before(){
        dag = new DAG();
        List<Vertex> vertexList = new LinkedList<>();
        final Process a = new Process("P1", 2);
        vertexList.add(a);
        dag.registerVertices(vertexList);
    }


    @Test
    public void test1(){

        String infiles[] = {
                "/tmp/nxf.3iuGDWr6Id;0;;4096;directory;-;2021-11-10 12:58:11.210414589 +0000;2021-11-10 12:58:11.222414603 +0000",
                "/tmp/nxf.3iuGDWr6Id/file.txt;1;/pvcdata/testfile.txt;6;regular file;-;2021-11-10 12:58:07.485035700 +0000;2021-11-10 12:58:07.219065500 +0000",
                "/tmp/nxf.3iuGDWr6Id/.command.err;1;;0;regular empty file;-;2021-11-10 12:58:11.222414603 +0000;2021-11-10 12:58:11.222414603 +0000",
                "/tmp/nxf.3iuGDWr6Id/.command.out;1;;0;regular empty file;-;2021-11-10 12:58:11.222414603 +0000;2021-11-10 12:58:11.222414603 +0000"
        };

        String[] outfiles = {
                "/localdata/localwork/1e/249602b469f33100bb4a65203cb650;0;;4096;directory;-;2021-11-10 12:58:11.278414667 +0000;2021-11-10 12:58:11.278414667 +0000",
                "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/file.txt;1;/pvcdata/testfile.txt;13;regular file;-;2021-11-10 12:58:11.230039000 +0000;2021-11-10 12:58:11.230039000 +0000",
                "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/file1.txt;1;/pvcdata/testfile.txt;13;regular file;-;2021-11-10 12:58:11.230039000 +0000;2021-11-10 12:58:11.230039000 +0000"
        };

        final Path path = storeData(infiles, outfiles);

        final TaskResultParser taskResultParser = new TaskResultParser();
        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(path, NodeLocation.getLocation("Node1"), dag.getByProcess("P1"), false);

        log.info("{}", newAndUpdatedFiles);

        final HashSet<Object> expected = new HashSet<>();
        expected.add( new PathLocationWrapperPair(Path.of("/pvcdata/testfile.txt"), new LocationWrapper(NodeLocation.getLocation("Node1"), 1636549091230l, 13, dag.getByProcess("P1"))) );
        expected.add( new SymlinkOutput( "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/file.txt", "/pvcdata/testfile.txt") );
        expected.add( new SymlinkOutput( "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/file1.txt", "/pvcdata/testfile.txt") );

        assertEquals( expected, newAndUpdatedFiles );

    }

    @Test
    public void test2(){

        String infiles[] = {
                "/tmp/nxf.IANFIlM3Kv;0;;4096;directory;-;2021-11-12 12:42:42.155614026 +0000;2021-11-12 12:42:42.171614019 +0000",
                "/tmp/nxf.IANFIlM3Kv/file.txt;1;/pvcdata/testfile.txt;0;regular empty file;-;2021-11-12 12:42:29.000000000 +0000;2021-11-12 12:42:29.000000000 +0000",
                "/tmp/nxf.IANFIlM3Kv/.command.err;1;;0;regular empty file;-;2021-11-12 12:42:42.171614019 +0000;2021-11-12 12:42:42.171614019 +0000",
                "/tmp/nxf.IANFIlM3Kv/.command.out;1;;0;regular empty file;-;2021-11-12 12:42:42.171614019 +0000;2021-11-12 12:42:42.171614019 +0000"
        };

        String[] outfiles = {
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa;0;;4096;directory;-;2021-11-12 12:42:42.239613991 +0000;2021-11-12 12:42:42.243613989 +0000",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t;1;;4096;directory;-;2021-11-12 12:42:42.223613997 +0000;2021-11-12 12:42:42.223613997 +0000",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/filenew.txt;1;;0;regular empty file;-;2021-11-12 12:42:42.223613997 +0000;2021-11-12 12:42:42.223613997 +0000",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/b.txt;1;;2;regular file;-;2021-11-12 12:42:42.223613997 +0000;2021-11-12 12:42:42.223613997 +0000",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/c.txt;1;;2;regular file;-;2021-11-12 12:42:42.223613997 +0000;2021-11-12 12:42:42.223613997 +0000",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/a.txt;1;;2;regular file;-;2021-11-12 12:42:42.223613997 +0000;2021-11-12 12:42:42.223613997 +0000"
        };

        final Path path = storeData(infiles, outfiles);

        final TaskResultParser taskResultParser = new TaskResultParser();
        final NodeLocation node1 = NodeLocation.getLocation("Node1");
        final Process p1 = dag.getByProcess("P1");
        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(path, node1, p1, false);

        log.info("{}", newAndUpdatedFiles.toArray());

        final HashSet<Object> expected = new HashSet<>();
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/filenew.txt"), new LocationWrapper(node1, 1636720962223l, 0, p1)) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/b.txt"), new LocationWrapper(node1, 1636720962223l, 2, p1)) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/c.txt"), new LocationWrapper(node1, 1636720962223l, 2, p1)) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/a.txt"), new LocationWrapper(node1, 1636720962223l, 2, p1)) );

        assertEquals( expected, newAndUpdatedFiles );

    }

    @Test
    public void test3(){

        String infiles[] = {
            "/tmp/nxf.9J6Y5mcXRD;0;;4096;directory;-;2021-11-12 14:42:20.941053482 +0000;2021-11-12 14:42:20.937053480 +0000",
            "/tmp/nxf.9J6Y5mcXRD/t;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t;4096;directory;-;2021-11-12 14:42:17.009050211 +0000;2021-11-12 14:42:16.973050180 +0000",
            "/tmp/nxf.9J6Y5mcXRD/t/filenew.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/filenew.txt;0;regular empty file;-;2021-11-12 14:42:16.969050176 +0000;2021-11-12 14:42:16.969050176 +0000",
            "/tmp/nxf.9J6Y5mcXRD/t/b.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/b.txt;2;regular file;-;2021-11-12 14:42:16.973050180 +0000;2021-11-12 14:42:16.973050180 +0000",
            "/tmp/nxf.9J6Y5mcXRD/t/c.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/c.txt;2;regular file;-;2021-11-12 14:42:16.973050180 +0000;2021-11-12 14:42:16.973050180 +0000",
            "/tmp/nxf.9J6Y5mcXRD/t/a.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/a.txt;2;regular file;-;2021-11-12 14:42:16.973050180 +0000;2021-11-12 14:42:16.973050180 +0000",
            "/tmp/nxf.9J6Y5mcXRD/.command.err;1;;0;regular empty file;-;2021-11-12 14:42:20.937053480 +0000;2021-11-12 14:42:20.937053480 +0000",
            "/tmp/nxf.9J6Y5mcXRD/.command.out;1;;0;regular empty file;-;2021-11-12 14:42:20.937053480 +0000;2021-11-12 14:42:20.937053480 +0000"
        };

        String[] outfiles = {
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6;0;;4096;directory;-;2021-11-12 14:42:21.005053535 +0000;2021-11-12 14:42:21.009053539 +0000",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t;4096;directory;-;2021-11-12 14:42:17.009050211 +0000;2021-11-12 14:42:16.973050180 +0000",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/filenew.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/filenew.txt;0;regular empty file;-;2021-11-12 14:42:16.969050176 +0000;2021-11-12 14:42:16.969050176 +0000",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/b.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/b.txt;2;regular file;-;2021-11-12 14:42:16.973050180 +0000;2021-11-12 14:42:16.973050180 +0000",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/c.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/c.txt;2;regular file;-;2021-11-12 14:42:16.973050180 +0000;2021-11-12 14:42:16.973050180 +0000",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/a.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/a.txt;4;regular file;-;2021-11-12 14:42:16.973050180 +0000;2021-11-12 14:42:20.993053525 +0000"
        };

        final Path path = storeData(infiles, outfiles);

        final TaskResultParser taskResultParser = new TaskResultParser();
        final NodeLocation node1 = NodeLocation.getLocation("Node1");
        final Process p1 = dag.getByProcess("P1");
        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(path, node1, p1, false);

        log.info("{}", newAndUpdatedFiles);

        final HashSet<Object> expected = new HashSet<>();
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/a.txt"), new LocationWrapper(node1, 1636728140993l, 4, p1)) );
        expected.add( new SymlinkOutput( "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/b.txt", "/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/b.txt") );
        expected.add( new SymlinkOutput( "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/c.txt", "/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/c.txt") );
        expected.add( new SymlinkOutput( "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/a.txt", "/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/a.txt") );
        expected.add( new SymlinkOutput( "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/filenew.txt", "/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/filenew.txt") );

        assertEquals( expected, newAndUpdatedFiles );

    }

}