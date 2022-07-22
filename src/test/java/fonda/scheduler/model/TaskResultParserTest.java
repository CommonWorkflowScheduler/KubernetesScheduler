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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import static org.junit.Assert.assertEquals;

@Slf4j
public class TaskResultParserTest {

    private final String NL = "\n";

    private Path storeData( String[] inputdata, String[] outputdata ){
        Path tmpDir = null;

        try {

            tmpDir = Files.createTempDirectory( "results" );
            log.info("Path: {}", tmpDir );
            BufferedWriter pw = new BufferedWriter( new FileWriter( tmpDir.resolve(".command.infiles").toString() ) );
            for (String s : inputdata) {
                pw.write( s );
                pw.write( '\n' );
            }
            pw.close();

            pw = new BufferedWriter( new FileWriter( tmpDir.resolve(".command.outfiles").toString() ) );
            for (String s : outputdata) {
                pw.write( s );
                pw.write( '\n' );
            }
            pw.close();
        } catch ( Exception ignored ){}
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

        String[] infiles = {
                "1636549091222254911",
                "/tmp/nxf.3iuGDWr6Id;1;;4096;directory",
                "file.txt;1;/pvcdata/testfile.txt;6;regular file",
                ".command.err;1;;0;regular empty file",
                ".command.out;1;;0;regular empty file"
        };

        String[] outfiles = {
                "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/",
                "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/file.txt;1;/pvcdata/testfile.txt;13;regular file;-;1636549091230400136;1636549091230935687",
                "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/file1.txt;1;/pvcdata/testfile.txt;13;regular file;-;1636549091230676133;1636549091230588932",
        };

        final Path path = storeData(infiles, outfiles);

        final Task task = new Task(new TaskConfig("P1"), dag);

        final TaskResultParser taskResultParser = new TaskResultParser();
        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(path, NodeLocation.getLocation("Node1"), false, task);

        log.info("{}", newAndUpdatedFiles);

        final HashSet<Object> expected = new HashSet<>();
        expected.add( new PathLocationWrapperPair(Path.of("/pvcdata/testfile.txt"), new LocationWrapper(NodeLocation.getLocation("Node1"), 1636549091230L, 13, task)) );
        expected.add( new SymlinkOutput( "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/file.txt", "/pvcdata/testfile.txt") );
        expected.add( new SymlinkOutput( "/localdata/localwork/1e/249602b469f33100bb4a65203cb650/file1.txt", "/pvcdata/testfile.txt") );

        assertEquals( expected, newAndUpdatedFiles );

    }

    @Test
    public void test2(){

        String[] infiles = {
                "1636720962171455407",
                "/tmp/nxf.IANFIlM3Kv;1;;4096;directory",
                "file.txt;1;/pvcdata/testfile.txt;0;regular empty file",
                ".command.err;1;;0;regular empty file",
                ".command.out;1;;0;regular empty file"
        };

        String[] outfiles = {
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t;1;;4096;directory;-;1636720962223377564;1636720962223927449",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/filenew.txt;1;;0;regular empty file;-;1636720962223648023;1636720962223304687",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/b.txt;1;;2;regular file;-;1636720962223821586;1636720962223246135",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/c.txt;1;;2;regular file;-;1636720962223035735;1636720962223994896",
                "/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/a.txt;1;;2;regular file;-;1636720962223163152;1636720962223785315"
        };

        final Path path = storeData(infiles, outfiles);

        final TaskResultParser taskResultParser = new TaskResultParser();
        final NodeLocation node1 = NodeLocation.getLocation("Node1");
        final Task task = new Task(  new TaskConfig("P1"), dag);
        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(path, node1, false, task);

        log.info("{}", newAndUpdatedFiles.toArray());

        final HashSet<Object> expected = new HashSet<>();
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/filenew.txt"), new LocationWrapper(node1, 1636720962223L, 0, task )) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/b.txt"), new LocationWrapper(node1, 1636720962223L, 2, task )) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/c.txt"), new LocationWrapper(node1, 1636720962223L, 2, task )) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/ac/fbbbb38f79bf684ddd54a3e190e8fa/t/a.txt"), new LocationWrapper(node1, 1636720962223L, 2, task )) );

        assertEquals( expected, newAndUpdatedFiles );

    }

    @Test
    public void test3(){

        final TaskResultParser taskResultParser = new TaskResultParser();

        String[] infiles = {
            "1636728138941757518",
            "/tmp/nxf.9J6Y5mcXRD;1;;directory",
            "t;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t;directory",
            "t/filenew.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/filenew.txt;regular empty file",
            "t/b.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/b.txt;regular file",
            "t/c.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/c.txt;regular file",
            "t/a.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/a.txt;regular file",
            ".command.err;1;;regular empty file",
            ".command.out;1;;regular empty file"
        };

        String[] outfiles = {
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t;4096;directory;-;1636728137009129688;1636728136973190140",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/filenew.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/filenew.txt;0;regular empty file;-;1636728136969424493;1636728136969738095",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/b.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/b.txt;2;regular file;-;1636728136973929300;1636728136973771278",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/c.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/c.txt;2;regular file;-;1636728136973979318;1636728136973691162",
            "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/a.txt;1;/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/a.txt;4;regular file;-;1636728136973542646;1636728140993004364"
        };

        final Path path = storeData(infiles, outfiles);

        final NodeLocation node1 = NodeLocation.getLocation("Node1");
        final Task task = new Task( new TaskConfig("P1"), dag );
        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(path, node1, false, task);

        log.info("{}", newAndUpdatedFiles);

        final HashSet<Object> expected = new HashSet<>();
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/a.txt"), new LocationWrapper( node1, 1636728140993L, 4, task )) );
        expected.add( new SymlinkOutput( "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/b.txt", "/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/b.txt") );
        expected.add( new SymlinkOutput( "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/c.txt", "/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/c.txt") );
        expected.add( new SymlinkOutput( "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/a.txt", "/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/a.txt") );
        expected.add( new SymlinkOutput( "/localdata/localwork/a2/f105825376b35dd6918824136adbf6/t/filenew.txt", "/localdata/localwork/3c/b1c1be1266dfd66b81a9942383e266/t/filenew.txt") );

        assertEquals( expected, newAndUpdatedFiles );

    }

    @Test
    public void test4(){

        String[] infiles = {
                "---",
                "---",
                "wvdb;1;/input/FORCE2NXF-Rangeland/inputdata/wvdb;0;directory",
                "wvdb/WVP_2020-12-16.txt;1;;420379;regular file",
        };

        final TaskResultParser taskResultParser = new TaskResultParser();
        Set<String> inputData = new HashSet();
        taskResultParser.processInput(Arrays.stream(infiles),inputData);
        final HashSet<String> expected = new HashSet<>();
        log.info("{}", inputData);
    }

    @Test
    public void test5(){

        final TaskResultParser taskResultParser = new TaskResultParser();

        String[] infiles = {
                "1656925624971202680",
                "/localdata/localwork/scratch/nxf.VX0s8jr100/;1;;directory",
                ".command.err;1;;regular file",
                ".command.out;1;;regular file"
        };

        String[] outfiles = {
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5;1;;4096;directory;1656925625099630891;1656925625099376329;1656925625219623172",
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5;1;;4096;directory;1656925625099699272;1656925625099839483;1656925625219393367",
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a;1;;4096;directory;1656925625151870586;1656925625151443598;1656925625207978673",
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/file.txt;1;;0;regular empty file;1656925625019383511;1656925625019813106;1656925625019125236",
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a/b;1;;4096;directory;1656925625163388439;1656925625163351190;1656925625195325883",
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a/file.txt;1;;0;regular empty file;1656925625023099305;1656925625023645725;1656925625023215656",
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a/b/c;1;;4096;directory;1656925625171561519;1656925625171633246;1656925625175783837",
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a/b/file.txt;1;;0;regular empty file;1656925625023206366;1656925625023824131;1656925625023970434",
                "/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a/b/c/file.txt;1;;0;regular empty file;1656925625023995796;1656925625023981665;1656925625023053106"
        };

        final Path path = storeData(infiles, outfiles);

        final NodeLocation node1 = NodeLocation.getLocation("Node1");
        final Task task = new Task( new TaskConfig("P1"), dag );
        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(path, node1, false, task);

        log.info("{}", newAndUpdatedFiles);

        final HashSet<Object> expected = new HashSet<>();
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/file.txt"), new LocationWrapper( node1, 1656925625019L, 0, task )) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a/file.txt"), new LocationWrapper( node1, 1656925625023L, 0, task )) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a/b/file.txt"), new LocationWrapper( node1, 1656925625023L, 0, task )) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/9c/33932e89127a7f1bb09bc2ca0453c5/a/b/c/file.txt"), new LocationWrapper( node1, 1656925625023L, 0, task )) );
        assertEquals( expected, newAndUpdatedFiles );

    }

    @Test
    public void test6(){

        final TaskResultParser taskResultParser = new TaskResultParser();

        String[] infiles = {
                "1658328555702440734",
                "/localdata/localwork/scratch/nxf.QqsTpl7jcy/",
                ".command.err;1;;0;regular file;1658328555691575355;1658328555691575355;1658328555691575355",
                "JK2782_TGGCCGATCAACGA_L008_R2_001.fastq.gz.tengrand.fq.gz;1;/pvcdata/work/stage/da/08d4344c92af6ca44284ba0e17b484/JK2782_TGGCCGATCAACGA_L008_R2_001.fastq.gz.tengrand.fq.gz;111;symbolic link;1658328555659575265;1658328555659575265;1658328555659575265",
                ".command.out;1;;0;regular file;1658328555691575355;1658328555691575355;1658328555691575355",
                "JK2782_TGGCCGATCAACGA_L008_R1_001.fastq.gz.tengrand.fq.gz;1;/pvcdata/work/stage/db/a8a5b9f6d77f6fa9878977a278bc69/JK2782_TGGCCGATCAACGA_L008_R1_001.fastq.gz.tengrand.fq.gz;111;symbolic link;1658328555651575241;1658328555651575241;1658328555651575241",

        };

        String[] outfiles = {
                "/localdata/localwork/30/adb97e8cffa8a086608565fb4c4ea9;1;;4096;directory;1658328570467617203;1658328570355616887;1658328570467617203",
                "/localdata/localwork/30/adb97e8cffa8a086608565fb4c4ea9/a;1;;249228;regular file;1658328570427617091;1658328569675614962;1658328569683614985",
                "/localdata/localwork/30/adb97e8cffa8a086608565fb4c4ea9/b;1;;260036;regular file;1658328570467617203;1658328559663586610;1658328569659614917",
                "/localdata/localwork/30/adb97e8cffa8a086608565fb4c4ea9/c;1;;265581;regular file;1658328570391616989;1658328559231585386;1658328569707615053",
        };

        Set<String> inputData = new HashSet();
        taskResultParser.processInput(Arrays.stream(infiles),inputData);
        log.info("{}", inputData);

        final Path path = storeData(infiles, outfiles);

        final NodeLocation node1 = NodeLocation.getLocation("Node1");
        final Task task = new Task( new TaskConfig("P1"), dag );
        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(path, node1, false, task);

        log.info("{}", newAndUpdatedFiles);

        final HashSet<Object> expected = new HashSet<>();
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/30/adb97e8cffa8a086608565fb4c4ea9/a"), new LocationWrapper( node1, 1658328569683L, 249228, task )) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/30/adb97e8cffa8a086608565fb4c4ea9/b"), new LocationWrapper( node1, 1658328569659L, 260036, task )) );
        expected.add( new PathLocationWrapperPair(Path.of("/localdata/localwork/30/adb97e8cffa8a086608565fb4c4ea9/c"), new LocationWrapper( node1, 1658328569707L, 265581, task )) );
        assertEquals( expected, newAndUpdatedFiles );

    }

    @Test
    public void test7(){
        long a = System.currentTimeMillis();
        long b = (long) (1658328570467617203l / 1.0E6);
        log.info("{} {}", a - b, b);
    }


    @Test
    public void fileWalker() throws IOException {
        FileVisitor<? super Path> visitor = new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                log.info("preVisitDirectory: {}", dir);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                log.info("visitFile: {}", file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                log.info("visitFileFailed: {}", file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                log.info("postVisitDirectory: {}", dir);
                return FileVisitResult.CONTINUE;
            }
        };
        log.info("{}", Files.walkFileTree(Paths.get("C:\\Users\\Fabian Lehmann\\Documents\\VBox\\Paper\\test"), EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, visitor));
    }

}