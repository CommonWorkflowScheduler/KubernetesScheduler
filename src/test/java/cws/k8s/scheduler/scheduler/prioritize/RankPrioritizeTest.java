package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.InputEdge;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RankPrioritizeTest {

    @Test
    void sortTasks() throws InterruptedException {

        final DAG dag = new DAG();
        final Process a = new Process("a", 1);
        final Process b = new Process("b", 2);
        final Process c = new Process("c", 3);
        List<Vertex> vertexList = Arrays.asList( a, b, c );
        List<InputEdge> inputEdges = new LinkedList<>();
        inputEdges.add( new InputEdge(1, 1,2) );
        inputEdges.add( new InputEdge(2, 2,3) );
        dag.registerVertices( vertexList );
        dag.registerEdges(inputEdges);

        final Task c1 = new Task( new TaskConfig( "c" ), dag );
        Thread.sleep( 10 );
        final Task b1 = new Task( new TaskConfig( "b" ), dag );
        Thread.sleep( 10 );
        final Task a1 = new Task( new TaskConfig( "a" ), dag );
        Thread.sleep( 10 );
        final Task a2 = new Task( new TaskConfig( "a" ), dag );
        Thread.sleep( 10 );
        final Task b2 = new Task( new TaskConfig( "b" ), dag );
        Thread.sleep( 10 );
        final Task c2 = new Task( new TaskConfig( "c" ), dag );
        Thread.sleep( 10 );

        final List<Task> tasks = Arrays.asList( c1, b1, a1, a2, b2, c2 );
        new RankPrioritize().sortTasks( tasks );
        assertEquals( Arrays.asList( a1, a2, b1, b2, c1, c2 ), tasks );


        final List<Task> tasks2 = Arrays.asList( c2, b2, a2, a1, b1, c1 );
        new RankPrioritize().sortTasks( tasks2 );
        assertEquals( Arrays.asList( a1, a2, b1, b2, c1, c2 ), tasks2 );

    }
}