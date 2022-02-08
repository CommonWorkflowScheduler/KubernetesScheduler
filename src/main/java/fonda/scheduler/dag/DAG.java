package fonda.scheduler.dag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DAG {

    private final Map<Integer, Vertex> vertices = new HashMap<>();
    private final Map<String, Process> processes = new HashMap<>();

    private Vertex getByUid( int uid ){
        final Vertex vertex = vertices.get(uid);
        if ( vertex == null ) throw new IllegalStateException( "Cannot find vertex with id " + uid );
        return vertex;
    }

    public Process getByProcess( String s ){
        return processes.get( s );
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
                final Edge edgeNew = new Edge(edge.getLabel(), getByUid(edge.getFrom()), getByUid(edge.getTo()));
                edgeNew.getFrom().addOutbound(edgeNew);
                edgeNew.getTo().addInbound(edgeNew);
            }
        }
    }

}
