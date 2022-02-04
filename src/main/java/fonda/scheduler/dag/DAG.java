package fonda.scheduler.dag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DAG {

    private Map<Integer, Vertex> vertices = new HashMap<>();

    private Vertex getByUid( int uid ){
        final Vertex vertex = vertices.get(uid);
        if ( vertex == null ) throw new IllegalStateException( "Cannot find vertex with id " + uid );
        return vertex;
    }

    public void registerVertices( List<Vertex> vertices ){
        for (Vertex vertex : vertices) {
            synchronized ( this.vertices ) {
                this.vertices.put( vertex.getUid(), vertex );
            }
        }
    }

    public void registerEdges( List<InputEdge> edges ){
        for (InputEdge edge : edges) {
            final Edge edgeNew = new Edge(edge.getLabel(), getByUid(edge.getFrom()), getByUid(edge.getTo()));
            edgeNew.getFrom().addOutbound( edgeNew );
            edgeNew.getTo().addInbound( edgeNew );
        }
    }

}
