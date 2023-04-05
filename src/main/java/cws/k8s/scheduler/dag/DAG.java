package cws.k8s.scheduler.dag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DAG {

    private final Map<Integer, Vertex> vertices = new HashMap<>();
    private final Map<Integer, Edge> edges = new HashMap<>();
    private final Map<String, Process> processes = new HashMap<>();

    /**
     * not private for testing
     */
    Vertex getByUid( int uid ){
        final Vertex vertex = vertices.get(uid);
        if ( vertex == null ) {
            throw new IllegalStateException( "Cannot find vertex with id " + uid );
        }
        return vertex;
    }

    public Process getByProcess( String s ){
        final Process process = processes.get(s);
        if ( process == null ){
            throw new IllegalStateException("Process can not be found! Searching: " + s + " processes: " + processes );
        }
        return process;
    }

    public void registerVertices( List<Vertex> vertices ){
        for (Vertex vertex : vertices) {
            synchronized ( this.vertices ) {
                this.vertices.put( vertex.getUid(), vertex );
            }
            if ( vertex.getType() == Type.PROCESS ) {
                synchronized ( this.processes ) {
                    this.processes.put( vertex.getLabel(), (Process) vertex );
                }
            }
        }
    }

    public void registerEdges( List<InputEdge> edges ){
        synchronized ( this.vertices ) {
            for (InputEdge edge : edges) {
                final Edge edgeNew = new Edge( edge.getUid(), edge.getLabel(), getByUid(edge.getFrom()), getByUid(edge.getTo()));
                synchronized ( this.edges ) {
                    this.edges.put( edgeNew.getUid(), edgeNew );
                }
                edgeNew.getFrom().addOutbound(edgeNew);
                edgeNew.getTo().addInbound(edgeNew);
            }
        }
    }

    /**
     * This method removes vertices from the DAG plus the edges.
     */
    public void removeVertices( int... verticesIds ){
        synchronized ( this.vertices ) {
            for ( int vertexId : verticesIds ) {
                final Vertex remove = this.vertices.remove(vertexId);
                if ( remove.getType() == Type.PROCESS ) {
                    processes.remove(remove.getLabel());
                }
                remove.deleteItself();
                synchronized ( this.edges ) {
                    for (Edge edge : remove.getIn()) {
                        this.edges.remove(edge.getUid());
                    }
                    for (Edge edge : remove.getOut()) {
                        this.edges.remove(edge.getUid());
                    }
                }
            }
        }
    }

    public void removeEdges( int... edgesIds ){
        synchronized ( this.vertices ) {
            for ( int edgeId : edgesIds ) {
                final Edge remove;
                synchronized ( this.edges ) {
                    remove = this.edges.remove( edgeId );
                }
                remove.getTo().removeInbound(remove);
            }
        }
    }

}
