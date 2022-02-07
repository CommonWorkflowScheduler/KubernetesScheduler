package fonda.scheduler.dag;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Slf4j
public class DAGTest {

    private void compare(int uid, List<Vertex> vertices, int[] ancestorIds, int[] descedantsIds ){
        final Vertex vertex = vertices.stream().filter(v -> v.getUid() == uid).findFirst().get();

        if( vertex.getAncestors() == null ){
            assertNull( ancestorIds );
        } else {
            final int[] a = vertex.getAncestors().stream().mapToInt( Vertex::getUid ).sorted().toArray();
            assertArrayEquals("Compare Ancestors for uid: " + uid, ancestorIds, a);
        }
        if( vertex.getDescendants() == null ){
            assertNull( descedantsIds );
        } else {
            final int[] d = vertex.getDescendants().stream().mapToInt(Vertex::getUid).sorted().toArray();
            assertArrayEquals("Compare Descendants for uid: " + uid, descedantsIds, d);
        }
    }

    public void expectedResult( List<Vertex> vertexList ) {
        compare( 1, vertexList, new int[]{}, new int[]{2,3,4,5,6,7,8,9,10,11,12} );
        compare( 2, vertexList, new int[]{1} , new int[]{6,8,10,12} );
        compare( 3, vertexList, new int[]{1}, new int[]{7,8,10,12} );
        compare( 4, vertexList, new int[]{1}, new int[]{7,8,9,10,11,12} );
        compare( 5, vertexList, new int[]{1}, new int[]{8,9,10,11,12} );
        compare( 6, vertexList, new int[]{1,2}, new int[]{8,10,12} );
        compare( 7, vertexList, new int[]{1,3,4}, new int[]{8,10,12} );
        compare( 8, vertexList, new int[]{1,2,3,4,5,6,7,9}, new int[]{10,12} );
        compare( 9, vertexList, new int[]{1,4,5}, new int[]{8,10,11,12} );
        compare( 10, vertexList, new int[]{1,2,3,4,5,6,7,8,9}, new int[]{12} );
        compare( 11, vertexList, new int[]{1,4,5,9}, new int[]{12} );
        compare( 12, vertexList, new int[]{1,2,3,4,5,6,7,8,9,10,11}, new int[]{} );
    }

    public List<Vertex> genVertexList(){
        List<Vertex> vertexList = new LinkedList<>();
        for (int i = 1; i <= 12; i++) {
            vertexList.add( new Vertex("" + (char) ('a' + i), Type.PROCESS, i) );
        }
        return vertexList;
    }

    public List<InputEdge> genEdgeList(){
        List<InputEdge> inputEdges = new LinkedList<>();
        inputEdges.add( new InputEdge(1,2) );
        inputEdges.add( new InputEdge(1,3) );
        inputEdges.add( new InputEdge(1,4) );
        inputEdges.add( new InputEdge(1,5) );
        inputEdges.add( new InputEdge(2,6) );
        inputEdges.add( new InputEdge(3,7) );
        inputEdges.add( new InputEdge(4,7) );
        inputEdges.add( new InputEdge(4,9) );
        inputEdges.add( new InputEdge(5,9) );
        inputEdges.add( new InputEdge(6,8) );
        inputEdges.add( new InputEdge(7,8) );
        inputEdges.add( new InputEdge(9,8) );
        inputEdges.add( new InputEdge(8,10) );
        inputEdges.add( new InputEdge(9,11) );
        inputEdges.add( new InputEdge(10,12) );
        inputEdges.add( new InputEdge(11,12) );

        Collections.shuffle( inputEdges );
        return inputEdges;
    }

    public void debug( DAG dag, int length ){
        for (int i = 1; i <= length; i++) {
            final Vertex process = dag.getByUid( i );
            log.info( process.toString() );
        }
    }

    @Test
    public void testRelations() {
        for (int q = 0; q < 500 ; q++) {
            final DAG dag = new DAG();
            List<Vertex> vertexList = genVertexList();
            dag.registerVertices( vertexList );
            List<InputEdge> inputEdges = genEdgeList();
            dag.registerEdges( inputEdges );
            debug( dag, 12 );
            expectedResult ( vertexList );
        }
    }

    @Test
    public void testRelations2() {
        for (int q = 0; q < 500 ; q++) {
            final DAG dag = new DAG();
            List<Vertex> vertexList = genVertexList();
            List<InputEdge> inputEdges = genEdgeList();
            for (int i = 0; i < 500; i++) {
                int index = new Random().nextInt( inputEdges.size() );
                final InputEdge remove = inputEdges.remove(index);
                final Vertex operator = new Vertex("Operator", Type.OPERATOR, vertexList.size() + 1 );
                vertexList.add( operator );
                inputEdges.add( new InputEdge( remove.getFrom(), operator.getUid() ) );
                inputEdges.add( new InputEdge( operator.getUid(), remove.getTo() ) );
            }
            dag.registerVertices( vertexList );
            dag.registerEdges( inputEdges );
            debug( dag, 12 );
            expectedResult ( vertexList );
        }
    }

    @Test
    public void smallTest(){

        final DAG dag = new DAG();

        final Vertex a = new Vertex("a", Type.PROCESS, 1);
        final Vertex filter = new Vertex("filter", Type.OPERATOR, 2);
        final Vertex b = new Vertex("b", Type.PROCESS, 3);

        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(a);
        vertexList.add(filter);
        vertexList.add(b);

        List<InputEdge> inputEdges = new LinkedList<>();
        inputEdges.add( new InputEdge(1,2) );
        inputEdges.add( new InputEdge(2,3) );

        dag.registerVertices( vertexList );
        dag.registerEdges( inputEdges );

        debug( dag, vertexList.size() );

        assertEquals( new HashSet<>(), a.getAncestors() );
        final HashSet descA = new HashSet<>();
        descA.add( b );
        assertEquals( descA, a.getDescendants() );


        final HashSet ancB = new HashSet<>();
        ancB.add( a );
        assertEquals( new HashSet<>(), b.getDescendants() );
        assertEquals( ancB, b.getAncestors() );

        assertEquals( ancB, filter.getAncestors() );
        assertEquals( descA, filter.getDescendants() );

    }


}