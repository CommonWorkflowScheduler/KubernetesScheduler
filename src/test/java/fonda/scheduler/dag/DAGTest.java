package fonda.scheduler.dag;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Slf4j
public class DAGTest {

    private void compare(int uid, List<Vertex> vertices, int[] ancestorIds, int[] descendantsIds ){
        //noinspection OptionalGetWithoutIsPresent
        final Vertex vertex = vertices.stream().filter(v -> v.getUid() == uid).findFirst().get();

        if( vertex.getAncestors() == null ){
            assertNull( ancestorIds );
        } else {
            final int[] a = vertex.getAncestors().stream().mapToInt( Vertex::getUid ).sorted().toArray();
            assertArrayEquals("Compare Ancestors for uid: " + uid, ancestorIds, a);
        }
        if( vertex.getDescendants() == null ){
            assertNull( descendantsIds );
        } else {
            final int[] d = vertex.getDescendants().stream().mapToInt(Vertex::getUid).sorted().toArray();
            assertArrayEquals("Compare Descendants for uid: " + uid, descendantsIds, d);
        }
    }

    /**
     * otherIds [edge id, destination id]
     * @param dag
     * @param idA
     * @param otherIds
     * @return
     */
    private HashSet<Edge> setFrom(DAG dag, int idA, int[]... otherIds){
        return Arrays.stream(otherIds).map(p -> {
            Vertex a = dag.getByUid(idA);
            Vertex b = dag.getByUid(p[1]);
            int edgeId = p[0];
            return new Edge(edgeId, a, b);
        }).collect(Collectors.toCollection(HashSet::new));
    }

    private HashSet<Edge> setTo(DAG dag, int idA, int[]... otherIds){
        return Arrays.stream(otherIds).map(p -> {
            Vertex a = dag.getByUid(idA);
            Vertex b = dag.getByUid(p[1]);
            int edgeId = p[0];
            return new Edge(edgeId, b, a);
        }).collect(Collectors.toCollection(HashSet::new));
    }

    private void isSameEdge(HashSet<Edge> expectedIn, HashSet<Edge> expectedOut, Vertex vertex ){
        final String[] in = setToArrayEdges(expectedIn);
        final String[] out = setToArrayEdges(expectedOut);
        assertArrayEquals( "In of " + vertex.getLabel(), in, setToArrayEdges(vertex.getIn()) );
        assertArrayEquals( "Out of " + vertex.getLabel(), out, setToArrayEdges(vertex.getOut()) );
    }

    private void isSameProc(HashSet<Process> expectedAncestors, HashSet<Process> expectedDescendants, Vertex vertex ){
        final String[] descendants = setToArrayProcesses(expectedDescendants);
        final String[] ancestors = setToArrayProcesses(expectedAncestors);
        assertArrayEquals( "Descendants of " + vertex.getLabel(), descendants, setToArrayProcesses(vertex.getDescendants()) );
        assertArrayEquals( "Ancestors of " + vertex.getLabel(), ancestors, setToArrayProcesses(vertex.getAncestors()) );
    }

    private String[] setToArrayProcesses(Set<Process> set) {
        return set.stream().map(Process::getLabel).sorted().collect(Collectors.toList()).toArray(new String[set.size()]);
    }

    private String[] setToArrayEdges(Set<Edge> set) {
        return set
                .stream()
                .map( x -> String.format("(uid:%d,from:%s,to:%s)", x.getUid(), x.getFrom().getLabel(),x.getTo().getLabel()))
                .sorted()
                .collect(Collectors.toList())
                .toArray(new String[set.size()]);
    }

    private HashSet<Process> set(DAG dag, String... elements){
        return Arrays.stream(elements).map(dag::getByProcess).collect(Collectors.toCollection(HashSet::new));
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
        inputEdges.add( new InputEdge(1,13,1) );
        inputEdges.add( new InputEdge(2,1,2) );
        inputEdges.add( new InputEdge(3,1,3) );
        inputEdges.add( new InputEdge(4,1,4) );
        inputEdges.add( new InputEdge(5,1,5) );
        inputEdges.add( new InputEdge(6,2,6) );
        inputEdges.add( new InputEdge(7,3,7) );
        inputEdges.add( new InputEdge(8,4,7) );
        inputEdges.add( new InputEdge(9,4,9) );
        inputEdges.add( new InputEdge(10,5,9) );
        inputEdges.add( new InputEdge(11,6,8) );
        inputEdges.add( new InputEdge(12,7,8) );
        inputEdges.add( new InputEdge(13,9,8) );
        inputEdges.add( new InputEdge(14,8,10) );
        inputEdges.add( new InputEdge(15,9,11) );
        inputEdges.add( new InputEdge(16,10,12) );
        inputEdges.add( new InputEdge(17,11,12) );

        Collections.shuffle( inputEdges );
        return inputEdges;
    }

    @Test
    public void testRelations() {
        for (int q = 0; q < 500 ; q++) {
            final DAG dag = new DAG();
            List<Vertex> vertexList = genVertexList();
            dag.registerVertices( vertexList );
            List<InputEdge> inputEdges = genEdgeList();
            dag.registerEdges( inputEdges );
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
                inputEdges.add( new InputEdge( i * 2, remove.getFrom(), operator.getUid() ) );
                inputEdges.add( new InputEdge( i * 2 + 1, operator.getUid(), remove.getTo() ) );
            }
            dag.registerVertices( vertexList );
            dag.registerEdges( inputEdges );
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
        inputEdges.add( new InputEdge(1,1,2) );
        inputEdges.add( new InputEdge(2,2,3) );
        inputEdges.add( new InputEdge(3,3,4) );

        dag.registerVertices( vertexList );
        dag.registerEdges( inputEdges );

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

    /**
     *
     *               o
     *               |
     *               a
     *              / \
     *             f  f2 <-- will be removed
     *             \  /
     *              b
     *
     */
    @Test
    public void deleteTest(){

        final DAG dag = new DAG();


        final Origin o = new Origin("o", 1);
        final Process a = new Process("a", 2);
        final Operator filter = new Operator("filter", 3);
        final Operator filter2 = new Operator("filter", 5);
        final Process b = new Process("b", 4);

        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(o);
        vertexList.add(a);
        vertexList.add(filter);
        vertexList.add(filter2);
        vertexList.add(b);

        List<InputEdge> inputEdges = new LinkedList<>();
        inputEdges.add( new InputEdge(1,1,2) );
        inputEdges.add( new InputEdge(2,2,3) );
        inputEdges.add( new InputEdge(3,2,5) );
        inputEdges.add( new InputEdge(4,3,4) );
        inputEdges.add( new InputEdge(5,5,4) );

        dag.registerVertices( vertexList );
        dag.registerEdges( inputEdges );

        for (int i = 0; i < 2; i++) {

            if ( i == 1 ) {
                dag.removeEdges( 3, 5 );
                dag.removeVertices( filter2.getUid() );
            }

            assertEquals( new HashSet<>(), a.getAncestors() );
            if ( i == 0 ) {
                assertEquals( new HashSet<>( Arrays.asList( new Edge(2,a,filter), new Edge(3,a,filter2) ) ), a.getOut() );
            } else {
                assertEquals( new HashSet<>(List.of(new Edge(2, a, filter))), a.getOut() );
            }
            final HashSet<Process> descA = new HashSet<>();
            descA.add( b );
            assertEquals( descA, a.getDescendants() );


            final HashSet<Process> ancB = new HashSet<>();
            ancB.add( a );
            assertEquals( new HashSet<>(), b.getDescendants() );
            assertEquals( ancB, b.getAncestors() );

            if ( i == 0 ) {
                assertEquals( new HashSet<>( Arrays.asList( new Edge(4,filter,b),  new Edge(5,filter2,b) ) ), b.getIn() );
            } else {
                assertEquals( new HashSet<>(List.of(new Edge(4, filter, b))), b.getIn() );
            }

            assertEquals( ancB, filter.getAncestors() );
            assertEquals( descA, filter.getDescendants() );

            assertEquals( new HashSet<>(),o.getAncestors() );
            final HashSet<Process> descO = new HashSet<>();

            descO.add( a );
            descO.add( b );
            assertEquals( descO, o.getDescendants() );
        }

    }




    @Test
    public void deleteTest2() {

        final DAG dag = createDag();
        dag.removeEdges( 3 );
        isSameProc(                                          new HashSet<>(),               set( dag, "a","b","c","d","e","f","g","h","i" ), dag.getByUid( 1 ) );
        isSameEdge(                                          new HashSet<>(),        setFrom( dag, 1, new int[]{2, 3},new int[]{1, 2} ), dag.getByUid( 1 ) );
        isSameProc(                                          new HashSet<>(),                                    set( dag, "g","i" ), dag.getByProcess("a") );
        isSameEdge(                     setTo( dag, 2, new int[]{1, 1} ),                 setFrom( dag, 2, new int[]{4, 8} ), dag.getByProcess("a") );
        isSameProc(                                          new HashSet<>(),                    set( dag, "c","d","e","f","h","i" ), dag.getByProcess("b") );
        isSameEdge(                     setTo( dag, 3, new int[]{2, 1} ), setFrom( dag, 3, new int[]{6, 5},new int[]{5, 4} ), dag.getByProcess("b") );
        isSameProc(                                          set( dag, "b" ),                                set( dag, "e","h","i" ), dag.getByProcess("c") );
        isSameEdge(                     setTo( dag, 4, new int[]{5, 3} ),                 setFrom( dag, 4, new int[]{7, 6} ), dag.getByProcess("c") );
        isSameProc(                                          set( dag, "b" ),                                set( dag, "f","h","i" ), dag.getByProcess("d") );
        isSameEdge(                     setTo( dag, 5, new int[]{6, 3} ),                 setFrom( dag, 5, new int[]{8, 7} ), dag.getByProcess("d") );
        isSameProc(                                      set( dag, "b","c" ),                                    set( dag, "h","i" ), dag.getByProcess("e") );
        isSameEdge(                     setTo( dag, 6, new int[]{7, 4} ),                 setFrom( dag, 6, new int[]{9, 9} ), dag.getByProcess("e") );
        isSameProc(                                      set( dag, "b","d" ),                                    set( dag, "h","i" ), dag.getByProcess("f") );
        isSameEdge(                     setTo( dag, 7, new int[]{8, 5} ),                setFrom( dag, 7, new int[]{10, 9} ), dag.getByProcess("f") );
        isSameProc(                                          set( dag, "a" ),                                        set( dag, "i" ), dag.getByProcess("g") );
        isSameEdge(                     setTo( dag, 8, new int[]{4, 2} ),               setFrom( dag, 8, new int[]{11, 10} ), dag.getByProcess("g") );
        isSameProc(                          set( dag, "b","c","d","e","f" ),                                        set( dag, "i" ), dag.getByProcess("h") );
        isSameEdge(    setTo( dag, 9, new int[]{10, 7},new int[]{9, 6} ),               setFrom( dag, 9, new int[]{12, 10} ), dag.getByProcess("h") );
        isSameProc(              set( dag, "a","b","c","d","e","f","g","h" ),                                        new HashSet<>(), dag.getByProcess("i") );
        isSameEdge(  setTo( dag, 10, new int[]{12, 9},new int[]{11, 8} ),                                        new HashSet<>(), dag.getByProcess("i") );

    }

    @Test
    public void deleteTest3() {

        final DAG dag = createDag();
        dag.removeVertices( dag.getByProcess("c").getUid(), dag.getByProcess("e").getUid() );
        isSameProc(                                         new HashSet<>(),                set( dag, "a","b","d","f","g","h","i" ), dag.getByUid( 1 ) );
        isSameEdge(                                         new HashSet<>(), setFrom( dag, 1, new int[]{2, 3},new int[]{1, 2} ), dag.getByUid( 1 ) );
        isSameProc(                                         new HashSet<>(),                              set( dag, "g","i" ), dag.getByProcess("a") );
        isSameEdge(                    setTo( dag, 2, new int[]{1, 1} ),           setFrom( dag, 2, new int[]{4, 8} ), dag.getByProcess("a") );
        isSameProc(                                         new HashSet<>(),                      set( dag, "d","f","h","i" ), dag.getByProcess("b") );
        isSameEdge(                    setTo( dag, 3, new int[]{2, 1} ),           setFrom( dag, 3, new int[]{6, 5} ), dag.getByProcess("b") );
        isSameProc(                                         set( dag, "b" ),                          set( dag, "f","h","i" ), dag.getByProcess("d") );
        isSameEdge(                    setTo( dag, 5, new int[]{6, 3} ),           setFrom( dag, 5, new int[]{8, 7} ), dag.getByProcess("d") );
        isSameProc(                                     set( dag, "b","d" ),                              set( dag, "h","i" ), dag.getByProcess("f") );
        isSameEdge(                    setTo( dag, 7, new int[]{8, 5} ),          setFrom( dag, 7, new int[]{10, 9} ), dag.getByProcess("f") );
        isSameProc(                                         set( dag, "a" ),                                  set( dag, "i" ), dag.getByProcess("g") );
        isSameEdge(                    setTo( dag, 8, new int[]{4, 2} ),         setFrom( dag, 8, new int[]{11, 10} ), dag.getByProcess("g") );
        isSameProc(                                 set( dag, "b","d","f" ),                                  set( dag, "i" ), dag.getByProcess("h") );
        isSameEdge(                   setTo( dag, 9, new int[]{10, 7} ),         setFrom( dag, 9, new int[]{12, 10} ), dag.getByProcess("h") );
        isSameProc(                     set( dag, "a","b","d","f","g","h" ),                                  new HashSet<>(), dag.getByProcess("i") );
        isSameEdge( setTo( dag, 10, new int[]{12, 9},new int[]{11, 8} ),                                  new HashSet<>(), dag.getByProcess("i") );
        assertThrows( IllegalStateException.class, () -> dag.getByProcess("c") );
        assertThrows( IllegalStateException.class, () -> dag.getByProcess("e") );

    }

    /**
     *      a
     *     / \
     *    b  c
     *    \ /|
     *     d |<--- delete this edge
     *     |/
     *     e
     */
    @Test
    public void deleteTest4() {

        final DAG dag = createDag();
        final Process a = new Process("a", 1);
        final Process b = new Process("b", 2);
        final Process c = new Process("c", 3);
        final Process d = new Process("d", 4);
        final Process e = new Process("e", 5);
        List<Vertex> vertexList = Arrays.asList( a, b, c, d, e );
        List<InputEdge> inputEdges = new LinkedList<>();
        inputEdges.add( new InputEdge(1, 1,2) );
        inputEdges.add( new InputEdge(2, 1,3) );
        inputEdges.add( new InputEdge(3, 2,4) );
        inputEdges.add( new InputEdge(4, 3,4) );
        inputEdges.add( new InputEdge(5, 3,5) );
        inputEdges.add( new InputEdge(6, 4,5) );
        dag.registerVertices( vertexList );
        dag.registerEdges(inputEdges);

        isSameProc(                                      new HashSet<>(),                            set( dag, "b","c","d","e" ), dag.getByProcess("a") );
        isSameEdge(                                      new HashSet<>(), setFrom( dag, 1, new int[]{2, 3},new int[]{1, 2} ), dag.getByProcess("a") );
        isSameProc(                                      set( dag, "a" ),                                    set( dag, "d","e" ), dag.getByProcess("b") );
        isSameEdge(                 setTo( dag, 2, new int[]{1, 1} ),                 setFrom( dag, 2, new int[]{3, 4} ), dag.getByProcess("b") );
        isSameProc(                                      set( dag, "a" ),                                    set( dag, "d","e" ), dag.getByProcess("c") );
        isSameEdge(                 setTo( dag, 3, new int[]{2, 1} ), setFrom( dag, 3, new int[]{4, 4},new int[]{5, 5} ), dag.getByProcess("c") );
        isSameProc(                              set( dag, "a","b","c" ),                                        set( dag, "e" ), dag.getByProcess("d") );
        isSameEdge( setTo( dag, 4, new int[]{4, 3},new int[]{3, 2} ),                 setFrom( dag, 4, new int[]{6, 5} ), dag.getByProcess("d") );
        isSameProc(                          set( dag, "a","b","c","d" ),                                        new HashSet<>(), dag.getByProcess("e") );
        isSameEdge( setTo( dag, 5, new int[]{5, 3},new int[]{6, 4} ),                                        new HashSet<>(), dag.getByProcess("e") );

        dag.removeEdges( 5 );

        isSameProc(                                      new HashSet<>(),                            set( dag, "b","c","d","e" ), dag.getByProcess("a") );
        isSameEdge(                                      new HashSet<>(), setFrom( dag, 1, new int[]{2, 3},new int[]{1, 2} ), dag.getByProcess("a") );
        isSameProc(                                      set( dag, "a" ),                                    set( dag, "d","e" ), dag.getByProcess("b") );
        isSameEdge(                 setTo( dag, 2, new int[]{1, 1} ),                 setFrom( dag, 2, new int[]{3, 4} ), dag.getByProcess("b") );
        isSameProc(                                      set( dag, "a" ),                                    set( dag, "d","e" ), dag.getByProcess("c") );
        isSameEdge(                 setTo( dag, 3, new int[]{2, 1} ),                setFrom( dag, 3, new int[]{4, 4} ), dag.getByProcess("c") );
        isSameProc(                              set( dag, "a","b","c" ),                                        set( dag, "e" ), dag.getByProcess("d") );
        isSameEdge( setTo( dag, 4, new int[]{4, 3},new int[]{3, 2} ),                 setFrom( dag, 4, new int[]{6, 5} ), dag.getByProcess("d") );
        isSameProc(                          set( dag, "a","b","c","d" ),                                        new HashSet<>(), dag.getByProcess("e") );
        isSameEdge(                 setTo( dag, 5, new int[]{6, 4} ),                                        new HashSet<>(), dag.getByProcess("e") );

    }

    /**
     *      a
     *     / \
     *    b  c
     *    \ /
     *     d
     *    | | <--- delete this edge
     *     e
     */
    @Test
    public void deleteTest5() {

        final DAG dag = new DAG();
        final Process a = new Process("a", 1);
        final Process b = new Process("b", 2);
        final Process c = new Process("c", 3);
        final Process d = new Process("d", 4);
        final Process e = new Process("e", 5);
        List<Vertex> vertexList = Arrays.asList( a, b, c, d, e );
        List<InputEdge> inputEdges = new LinkedList<>();
        inputEdges.add( new InputEdge(1, 1,2) );
        inputEdges.add( new InputEdge(2, 1,3) );
        inputEdges.add( new InputEdge(3, 2,4) );
        inputEdges.add( new InputEdge(4, 3,4) );
        inputEdges.add( new InputEdge(5, 4,5) );
        inputEdges.add( new InputEdge(6, 4,5) );
        dag.registerVertices( vertexList );
        dag.registerEdges(inputEdges);

        isSameProc(                                      new HashSet<>(),                            set( dag, "b","c","d","e" ), dag.getByProcess("a") );
        isSameEdge(                                      new HashSet<>(), setFrom( dag, 1, new int[]{2, 3},new int[]{1, 2} ), dag.getByProcess("a") );
        isSameProc(                                      set( dag, "a" ),                                    set( dag, "d","e" ), dag.getByProcess("b") );
        isSameEdge(                 setTo( dag, 2, new int[]{1, 1} ),                 setFrom( dag, 2, new int[]{3, 4} ), dag.getByProcess("b") );
        isSameProc(                                      set( dag, "a" ),                                    set( dag, "d","e" ), dag.getByProcess("c") );
        isSameEdge(                 setTo( dag, 3, new int[]{2, 1} ),                 setFrom( dag, 3, new int[]{4, 4} ), dag.getByProcess("c") );
        isSameProc(                              set( dag, "a","b","c" ),                                        set( dag, "e" ), dag.getByProcess("d") );
        isSameEdge( setTo( dag, 4, new int[]{4, 3},new int[]{3, 2} ), setFrom( dag, 4, new int[]{6, 5},new int[]{5, 5} ), dag.getByProcess("d") );
        isSameProc(                          set( dag, "a","b","c","d" ),                                        new HashSet<>(), dag.getByProcess("e") );
        isSameEdge( setTo( dag, 5, new int[]{6, 4},new int[]{5, 4} ),                                        new HashSet<>(), dag.getByProcess("e") );

        dag.removeEdges( 5 );

        isSameProc(                                      new HashSet<>(),                            set( dag, "b","c","d","e" ), dag.getByProcess("a") );
        isSameEdge(                                      new HashSet<>(), setFrom( dag, 1, new int[]{2, 3},new int[]{1, 2} ), dag.getByProcess("a") );
        isSameProc(                                      set( dag, "a" ),                                    set( dag, "d","e" ), dag.getByProcess("b") );
        isSameEdge(                 setTo( dag, 2, new int[]{1, 1} ),                 setFrom( dag, 2, new int[]{3, 4} ), dag.getByProcess("b") );
        isSameProc(                                      set( dag, "a" ),                                    set( dag, "d","e" ), dag.getByProcess("c") );
        isSameEdge(                 setTo( dag, 3, new int[]{2, 1} ),                 setFrom( dag, 3, new int[]{4, 4} ), dag.getByProcess("c") );
        isSameProc(                              set( dag, "a","b","c" ),                                        set( dag, "e" ), dag.getByProcess("d") );
        isSameEdge( setTo( dag, 4, new int[]{4, 3},new int[]{3, 2} ),                 setFrom( dag, 4, new int[]{6, 5} ), dag.getByProcess("d") );
        isSameProc(                          set( dag, "a","b","c","d" ),                                        new HashSet<>(), dag.getByProcess("e") );
        isSameEdge(                 setTo( dag, 5, new int[]{6, 4} ),                                        new HashSet<>(), dag.getByProcess("e") );

        dag.removeEdges( 6 );

        isSameProc(                                      new HashSet<>(),                                set( dag, "b","c","d" ), dag.getByProcess("a") );
        isSameEdge(                                      new HashSet<>(), setFrom( dag, 1, new int[]{2, 3},new int[]{1, 2} ), dag.getByProcess("a") );
        isSameProc(                                      set( dag, "a" ),                                        set( dag, "d" ), dag.getByProcess("b") );
        isSameEdge(                 setTo( dag, 2, new int[]{1, 1} ),                setFrom( dag, 2, new int[]{3, 4} ), dag.getByProcess("b") );
        isSameProc(                                      set( dag, "a" ),                                        set( dag, "d" ), dag.getByProcess("c") );
        isSameEdge(                 setTo( dag, 3, new int[]{2, 1} ),                 setFrom( dag, 3, new int[]{4, 4} ), dag.getByProcess("c") );
        isSameProc(                              set( dag, "a","b","c" ),                                        new HashSet<>(), dag.getByProcess("d") );
        isSameEdge( setTo( dag, 4, new int[]{4, 3},new int[]{3, 2} ),                                        new HashSet<>(), dag.getByProcess("d") );
        isSameProc(                                      new HashSet<>(),                                        new HashSet<>(), dag.getByProcess("e") );
        isSameEdge(                                      new HashSet<>(),                                        new HashSet<>(), dag.getByProcess("e") );

    }

    /**
     * o
     * | \
     * a   b
     * |\ |\
     * | -c d
     * |  | |
     * |  e f
     * |  |/
     * g  h
     * | /
     * i
     *
     * @return the dag
     */
    private DAG createDag() {
        final DAG dag = new DAG();

        final Origin o = new Origin("o", 1);
        final Process a = new Process("a", 2);
        final Process b = new Process("b", 3);
        final Process c = new Process("c", 4);
        final Process d = new Process("d", 5);
        final Process e = new Process("e", 6);
        final Process f = new Process("f", 7);
        final Process g = new Process("g", 8);
        final Process h = new Process("h", 9);
        final Process i = new Process("i", 10);

        List<Vertex> vertexList = Arrays.asList(o, a, b, c, d, e, f, g, h, i);

        List<InputEdge> inputEdges = Arrays.asList(
                new InputEdge( 1, o.getUid(), a.getUid()),
                new InputEdge( 2, o.getUid(), b.getUid()),
                new InputEdge( 3, a.getUid(), c.getUid()),
                new InputEdge( 4, a.getUid(), g.getUid()),
                new InputEdge( 5, b.getUid(), c.getUid()),
                new InputEdge( 6, b.getUid(), d.getUid()),
                new InputEdge( 7, c.getUid(), e.getUid()),
                new InputEdge( 8, d.getUid(), f.getUid()),
                new InputEdge( 9, e.getUid(), h.getUid()),
                new InputEdge( 10, f.getUid(), h.getUid()),
                new InputEdge( 11, g.getUid(), i.getUid()),
                new InputEdge( 12, h.getUid(), i.getUid())
        );

        dag.registerVertices(vertexList);
        dag.registerEdges(inputEdges);

        isSameProc(                                         new HashSet<>(),            set( dag, "a","b","c","d","e","f","g","h","i" ), o );
        isSameEdge(                                         new HashSet<>(),     setFrom( dag, 1, new int[]{2, 3},new int[]{1, 2} ), o );
        isSameProc(                                         new HashSet<>(),                            set( dag, "c","e","g","h","i" ), a );
        isSameEdge(                    setTo( dag, 2, new int[]{1, 1} ),     setFrom( dag, 2, new int[]{3, 4},new int[]{4, 8} ), a );
        isSameProc(                                         new HashSet<>(),                        set( dag, "c","d","e","f","h","i" ), b );
        isSameEdge(                    setTo( dag, 3, new int[]{2, 1} ),     setFrom( dag, 3, new int[]{6, 5},new int[]{5, 4} ), b );
        isSameProc(                                     set( dag, "a","b" ),                                    set( dag, "e","h","i" ), c );
        isSameEdge(    setTo( dag, 4, new int[]{5, 3},new int[]{3, 2} ),                     setFrom( dag, 4, new int[]{7, 6} ), c );
        isSameProc(                                         set( dag, "b" ),                                    set( dag, "f","h","i" ), d );
        isSameEdge(                    setTo( dag, 5, new int[]{6, 3} ),                     setFrom( dag, 5, new int[]{8, 7} ), d );
        isSameProc(                                 set( dag, "a","b","c" ),                                        set( dag, "h","i" ), e );
        isSameEdge(                    setTo( dag, 6, new int[]{7, 4} ),                     setFrom( dag, 6, new int[]{9, 9} ), e );
        isSameProc(                                     set( dag, "b","d" ),                                        set( dag, "h","i" ), f );
        isSameEdge(                    setTo( dag, 7, new int[]{8, 5} ),                    setFrom( dag, 7, new int[]{10, 9} ), f );
        isSameProc(                                         set( dag, "a" ),                                            set( dag, "i" ), g );
        isSameEdge(                    setTo( dag, 8, new int[]{4, 2} ),                   setFrom( dag, 8, new int[]{11, 10} ), g );
        isSameProc(                     set( dag, "a","b","c","d","e","f" ),                                            set( dag, "i" ), h );
        isSameEdge(   setTo( dag, 9, new int[]{10, 7},new int[]{9, 6} ),                   setFrom( dag, 9, new int[]{12, 10} ), h );
        isSameProc(             set( dag, "a","b","c","d","e","f","g","h" ),                                            new HashSet<>(), i );
        isSameEdge( setTo( dag, 10, new int[]{12, 9},new int[]{11, 8} ),                                            new HashSet<>(), i );
        return dag;
    }

}