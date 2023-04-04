package cws.k8s.scheduler.model;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.InputEdge;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.location.hierachy.*;
import cws.k8s.scheduler.model.taskinputs.PathFileLocationTriple;
import cws.k8s.scheduler.model.taskinputs.SymlinkInput;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

@Slf4j
public class InputFileCollectorTest {

    private LocationWrapper location11;
    private LocationWrapper location12;
    private LocationWrapper location21;
    private LocationWrapper location23;
    private LocationWrapper location34;
    private LocationWrapper location35;
    private LocationWrapper location44;
    private LocationWrapper location45;

    @Before
    public void init(){
        location11 = new LocationWrapper( NodeLocation.getLocation("Node1"), 1, 100);
        location12 = new LocationWrapper(NodeLocation.getLocation("Node2"), 1, 101);
        location21 = new LocationWrapper(NodeLocation.getLocation("Node1"), 1, 102);
        location23 = new LocationWrapper(NodeLocation.getLocation("Node3"), 1, 103);
        location34 = new LocationWrapper(NodeLocation.getLocation("Node4"), 1, 104);
        location35 = new LocationWrapper(NodeLocation.getLocation("Node5"), 1, 105);
        location44 = new LocationWrapper(NodeLocation.getLocation("Node4"), 1, 106);
        location45 = new LocationWrapper(NodeLocation.getLocation("Node5"), 1, 107);
    }

    @Test
    public void getInputsOfTaskTest() throws NoAlignmentFoundException {

        final String root = "/workdir/00/db62d739d658b839f07a1a77d877df/";
        final String root2 = "/workdir/01/db62d739d658b839f07a1a77d877d1/";
        final String root3 = "/workdir/02/db62d739d658b839f07a1a77d877d2/";

        final HierarchyWrapper hierarchyWrapper = new HierarchyWrapper("/workdir/");

        final Path path1 = Paths.get(root + "a.txt");
        assertNotNull( hierarchyWrapper.addFile(path1, location11) );
        assertNotNull( hierarchyWrapper.addFile(path1, location12) );
        final PathFileLocationTriple file1 = new PathFileLocationTriple(path1, (RealHierarchyFile) hierarchyWrapper.getFile(path1), List.of(location11, location12));

        final Path path2 = Paths.get(root + "a/b.txt");
        assertNotNull( hierarchyWrapper.addFile(path2, location21) );
        assertNotNull( hierarchyWrapper.addFile(path2, location23) );
        final PathFileLocationTriple file2 = new PathFileLocationTriple(path2, (RealHierarchyFile) hierarchyWrapper.getFile(path2), List.of(location21, location23));

        final Path path3 = Paths.get(root2 + "a/b.txt");
        assertNotNull( hierarchyWrapper.addFile(path3, location34) );
        assertNotNull( hierarchyWrapper.addFile(path3, location35) );
        final PathFileLocationTriple file3 = new PathFileLocationTriple(path3, (RealHierarchyFile) hierarchyWrapper.getFile(path3), List.of(location34, location35));

        final Path path4 = Paths.get(root3 + "b/c.txt");
        assertNotNull( hierarchyWrapper.addFile(path4, location44) );
        assertNotNull( hierarchyWrapper.addFile(path4, location45) );
        final PathFileLocationTriple file4 = new PathFileLocationTriple(path4, (RealHierarchyFile) hierarchyWrapper.getFile(path4), List.of(location44, location45));

        final Path path5 = Paths.get(root2 + "b/c.txt");
        assertTrue(hierarchyWrapper.addSymlink( path5, path4 ) );
        final SymlinkInput symlink = new SymlinkInput( path5, path4 );


        InputFileCollector inputFileCollector = new InputFileCollector( hierarchyWrapper );
        DAG dag = new DAG();
        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(new Process("processA", 1));
        dag.registerVertices(vertexList);

        final TaskConfig taskConfig = new TaskConfig("processA");
        final List<InputParam<FileHolder>> fileInputs = taskConfig.getInputs().fileInputs;
        final InputParam<FileHolder> a = new InputParam<>("a", new FileHolder(null, root, null));
        final InputParam<FileHolder> b = new InputParam<>("b", new FileHolder(null, root2 + "a/b.txt", null));

        fileInputs.add( a );
        fileInputs.add( b );
        final Task task = new Task(taskConfig, dag);

        final Set<PathFileLocationTriple> expected = new HashSet<>(List.of(file1, file2, file3));

        TaskInputs inputsOfTask = inputFileCollector.getInputsOfTask(task, Integer.MAX_VALUE);
        List<PathFileLocationTriple> inputsOfTaskFiles = inputsOfTask.getFiles();
        assertEquals( 3, inputsOfTaskFiles.size() );
        assertEquals( expected, new HashSet<>(inputsOfTaskFiles) );
        assertTrue( inputsOfTask.getSymlinks().isEmpty() );
        assertTrue( inputsOfTask.getExcludedNodes().isEmpty() );

        //Not existing file
        final InputParam<FileHolder> c = new InputParam<>("c", new FileHolder(null, root2 + "a.txt", null));
        fileInputs.add( c );

        inputsOfTask = inputFileCollector.getInputsOfTask(task, Integer.MAX_VALUE);
        inputsOfTaskFiles = inputsOfTask.getFiles();
        assertEquals( 3, inputsOfTaskFiles.size() );
        assertEquals( expected, new HashSet<>(inputsOfTaskFiles) );
        assertTrue( inputsOfTask.getSymlinks().isEmpty() );
        assertTrue( inputsOfTask.getExcludedNodes().isEmpty() );

        //Symlink
        final InputParam<FileHolder> d = new InputParam<>("d", new FileHolder(null, root2 + "b/c.txt", null));
        fileInputs.add( d );
        expected.add( file4 );

        inputsOfTask = inputFileCollector.getInputsOfTask(task, Integer.MAX_VALUE);
        inputsOfTaskFiles = inputsOfTask.getFiles();
        assertEquals( 4, inputsOfTaskFiles.size() );
        assertEquals( expected, new HashSet<>(inputsOfTaskFiles) );
        assertEquals( Set.of( symlink ), new HashSet<>(inputsOfTask.getSymlinks()) );
        assertTrue( inputsOfTask.getExcludedNodes().isEmpty() );


    }

    @Test
    public void getInputsOfTaskTestExcludeNodes() throws NoAlignmentFoundException {

        final String root = "/workdir/00/db62d739d658b839f07a1a77d877df/";

        final HierarchyWrapper hierarchyWrapper = new HierarchyWrapper("/workdir/");

        InputFileCollector inputFileCollector = new InputFileCollector( hierarchyWrapper );
        DAG dag = new DAG();
        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(new Process("processA", 1));
        vertexList.add(new Process("processB", 2));
        vertexList.add(new Process("processC", 3));
        dag.registerVertices(vertexList);
        dag.registerEdges( List.of( new InputEdge(1,1,2),new InputEdge(2,2,3) ) );

        final TaskConfig taskConfigA = new TaskConfig("processA");
        final Task taskA = new Task(taskConfigA, dag);

        final TaskConfig taskConfigB = new TaskConfig("processB");
        final Task taskB = new Task(taskConfigB, dag);

        final TaskConfig taskConfigC = new TaskConfig("processC");
        final List<InputParam<FileHolder>> fileInputs = taskConfigC.getInputs().fileInputs;
        final InputParam<FileHolder> a = new InputParam<>("a.txt", new FileHolder(null, root, null));
        fileInputs.add( a );
        final Task taskC = new Task(taskConfigC, dag);

        LocationWrapper location13 = new LocationWrapper(NodeLocation.getLocation("Node3"), 2, 102, taskB);
        LocationWrapper location12 = new LocationWrapper(NodeLocation.getLocation("Node2"), 2, 102, taskC);
        LocationWrapper location11 = new LocationWrapper(NodeLocation.getLocation("Node1"), 2, 102, taskA);
        location11.use();

        final Path path1 = Paths.get(root + "a.txt");
        assertNotNull( hierarchyWrapper.addFile(path1, location11) );
        assertNotNull( hierarchyWrapper.addFile(path1, location12) );
        assertNotNull( hierarchyWrapper.addFile(path1, location13) );
        final HierarchyFile file = hierarchyWrapper.getFile(path1);
        assertNotNull( file );
        final PathFileLocationTriple file1 = new PathFileLocationTriple(path1, (RealHierarchyFile) file, List.of(location12, location13));

        TaskInputs inputsOfTask = inputFileCollector.getInputsOfTask(taskC, Integer.MAX_VALUE);
        List<PathFileLocationTriple> inputsOfTaskFiles = inputsOfTask.getFiles();
        assertEquals( 1, inputsOfTaskFiles.size() );
        assertEquals( file1, inputsOfTaskFiles.get(0) );
        assertTrue( inputsOfTask.getSymlinks().isEmpty() );
        assertEquals( 1, inputsOfTask.getExcludedNodes().size() );
        Assert.assertEquals( NodeLocation.getLocation( "Node1" ), inputsOfTask.getExcludedNodes().iterator().next() );

    }

    @Test
    public void getInputsOfTaskTestNotExcludeNodes() throws NoAlignmentFoundException {

        final String root = "/workdir/00/db62d739d658b839f07a1a77d877df/";

        final HierarchyWrapper hierarchyWrapper = new HierarchyWrapper("/workdir/");

        InputFileCollector inputFileCollector = new InputFileCollector( hierarchyWrapper );
        DAG dag = new DAG();
        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(new Process("processA", 1));
        vertexList.add(new Process("processB", 2));
        vertexList.add(new Process("processC", 3));
        dag.registerVertices(vertexList);
        dag.registerEdges( List.of( new InputEdge(1,1,2),new InputEdge(2,2,3) ) );

        final TaskConfig taskConfigA = new TaskConfig("processA");
        final Task taskA = new Task(taskConfigA, dag);

        final TaskConfig taskConfigB = new TaskConfig("processB");
        final Task taskB = new Task(taskConfigB, dag);

        final TaskConfig taskConfigC = new TaskConfig("processC");
        final List<InputParam<FileHolder>> fileInputs = taskConfigC.getInputs().fileInputs;
        final InputParam<FileHolder> a = new InputParam<>("a.txt", new FileHolder(null, root, null));
        fileInputs.add( a );
        final Task taskC = new Task(taskConfigC, dag);

        LocationWrapper location13 = new LocationWrapper(NodeLocation.getLocation("Node3"), 2, 102, taskB);
        LocationWrapper location12 = new LocationWrapper(NodeLocation.getLocation("Node2"), 2, 102, taskC);
        LocationWrapper location11 = new LocationWrapper(NodeLocation.getLocation("Node1"), 2, 102, taskA);
        location13.use();

        final Path path1 = Paths.get(root + "a.txt");
        assertNotNull( hierarchyWrapper.addFile(path1, location11) );
        assertNotNull( hierarchyWrapper.addFile(path1, location12) );
        assertNotNull( hierarchyWrapper.addFile(path1, location13) );
        final HierarchyFile file = hierarchyWrapper.getFile(path1);
        assertNotNull( file );
        final PathFileLocationTriple file1 = new PathFileLocationTriple(path1, (RealHierarchyFile) file, List.of(location12, location13));
        log.info(file1.toString());

        TaskInputs inputsOfTask = inputFileCollector.getInputsOfTask(taskC, Integer.MAX_VALUE);
        List<PathFileLocationTriple> inputsOfTaskFiles = inputsOfTask.getFiles();
        assertEquals( 1, inputsOfTaskFiles.size() );
        assertEquals( file1, inputsOfTaskFiles.get(0) );
        assertTrue( inputsOfTask.getSymlinks().isEmpty() );
        assertTrue( inputsOfTask.getExcludedNodes().isEmpty() );

    }

}