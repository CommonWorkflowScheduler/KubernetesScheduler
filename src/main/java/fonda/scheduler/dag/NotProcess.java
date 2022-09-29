package fonda.scheduler.dag;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class NotProcess extends Vertex {

    NotProcess(String label, int uid) {
        super(label, uid);
    }

    public Set<Process> getDescendants() {
        final HashSet<Process> results = new HashSet<>();
        if ( !out.isEmpty() ) {
            for ( Edge edge : out ) {
                final Vertex to = edge.getTo();
                if ( to.getType() == Type.PROCESS ) {
                    results.add( (Process) to );
                }
                results.addAll( to.getDescendants() );
            }
        }
        return results;
    }

    public void addOutbound( Edge e ) {
        out.add( e );
        final Vertex to = e.getTo();
        final Set<Process> descendants = to.getDescendants();
        if ( to.getType() == Type.PROCESS ) {
            descendants.add( (Process) to );
        }
        final Set<Process> ancestors = this.getAncestors();
        descendants.forEach( v -> v.addAncestor( ancestors ) );
    }

    void removeDescendant( Edge e, Collection<Process> p ) {
        for ( Edge edge : in ) {
            edge.getFrom().removeDescendant( edge, p );
        }
    }


    void removeAncestor( Edge e, Collection<Process> p ) {
        for ( Edge edge : out ) {
            edge.getTo().removeAncestor( edge, p );
        }
    }

}
