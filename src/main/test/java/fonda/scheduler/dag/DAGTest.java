package fonda.scheduler.dag;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

@Slf4j
public class DAGTest {

    private void compare(int uid, List<Vertex> vertices, int[] ancestorIds, int[] descedantsIds ){
        //noinspection OptionalGetWithoutIsPresent
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
        int i = 1;
        for (; i <= 12; i++) {
            vertexList.add( new Process("" + (char) ('a' + i), i) );
        }
        vertexList.add( new Origin("Origin", 13 ) );
        return vertexList;
    }

    public List<InputEdge> genEdgeList(){
        List<InputEdge> inputEdges = new LinkedList<>();
        inputEdges.add( new InputEdge(13,1) );
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

    public void debug( List<Vertex> vertices, int start, int end ){
        for (int i = start; i <= end; i++) {
            final Vertex process = vertices.get( i ) ;
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
            debug( vertexList, 0, 11 );
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
                final Vertex operator = new Operator("Operator", vertexList.size() + 1 );
                vertexList.add( operator );
                inputEdges.add( new InputEdge( remove.getFrom(), operator.getUid() ) );
                inputEdges.add( new InputEdge( operator.getUid(), remove.getTo() ) );
            }
            dag.registerVertices( vertexList );
            dag.registerEdges( inputEdges );
            debug( vertexList, 0, 11 );
            expectedResult ( vertexList );
        }
    }

    @Test
    public void smallTest(){

        final DAG dag = new DAG();


        final Origin o = new Origin("o", 1);
        final Process a = new Process("a", 2);
        final Operator filter = new Operator("filter", 3);
        final Process b = new Process("b", 4);

        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(o);
        vertexList.add(a);
        vertexList.add(filter);
        vertexList.add(b);

        List<InputEdge> inputEdges = new LinkedList<>();
        inputEdges.add( new InputEdge(1,2) );
        inputEdges.add( new InputEdge(2,3) );
        inputEdges.add( new InputEdge(3,4) );

        dag.registerVertices( vertexList );
        dag.registerEdges( inputEdges );

        debug( vertexList, 0, 3 );

        assertEquals( new HashSet<>(), a.getAncestors() );
        final HashSet<Process> descA = new HashSet<>();
        descA.add( b );
        assertEquals( descA, a.getDescendants() );


        final HashSet<Process> ancB = new HashSet<>();
        ancB.add( a );
        assertEquals( new HashSet<>(), b.getDescendants() );
        assertEquals( ancB, b.getAncestors() );

        assertEquals( ancB, filter.getAncestors() );
        assertEquals( descA, filter.getDescendants() );

        assertEquals( new HashSet<>(),o.getAncestors() );
        final HashSet<Process> descO = new HashSet<>();

        descO.add( a );
        descO.add( b );
        assertEquals( descO, o.getDescendants() );

    }


}