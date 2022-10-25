package fonda.scheduler.dag;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class Process extends Vertex {


    private final Set<Process> descendants;
    private final Set<Process> ancestors;

    void addDescendant( Process p ) {
        synchronized (descendants) {
            descendants.add( p );
        }
    }

    void addDescendant( Collection<Process> p ) {
        synchronized (descendants) {
            descendants.addAll( p );
        }
    }

    void removeDescendant( Edge e, Collection<Process> pp ) {
        synchronized (descendants) {
            LinkedList<Process> toRemove = new LinkedList<>();
            for (Process p : pp) {
                boolean found = false;
                for (Edge edge : out) {
                    if ( edge != e && (edge.getTo().getDescendants().contains( p ) || edge.getTo() == p )) {
                        found = true;
                        break;
                    }
                }
                if ( !found ) {
                    descendants.remove( p );
                    toRemove.add( p );
                }
            }
            if ( !toRemove.isEmpty() ) {
                for (Edge edge : in) {
                    edge.getFrom().removeDescendant( edge, toRemove );
                }
            }
        }
    }

    void addAncestor( Process p ) {
        synchronized (ancestors) {
            ancestors.add( p );
        }
    }

    void addAncestor( Collection<Process> p ) {
        synchronized (ancestors) {
            ancestors.addAll( p );
        }
    }

    void removeAncestor( Edge e, Collection<Process> pp ) {
        synchronized (ancestors) {
            LinkedList<Process> toRemove = new LinkedList<>();
            for (Process p : pp) {
                boolean found = false;
                for (Edge edge : in) {
                    if ( edge != e && (edge.getFrom().getAncestors().contains( p ) || edge.getFrom() == p) ) {
                        found = true;
                        break;
                    }
                }
                if ( !found ) {
                    ancestors.remove( p );
                    toRemove.add( p );
                }
            }
            for (Edge edge : out) {
                edge.getTo().removeAncestor( edge, toRemove );
            }
        }
    }

    /**
     * Only public for tests
     * @param label
     * @param uid
     */
    public Process(String label, int uid) {
        super(label, uid);
        descendants = new HashSet<>();
        ancestors = new HashSet<>();
    }

    @Override
    public Type getType() {
        return Type.PROCESS;
    }

    public Set<Process> getDescendants() {
        return new HashSet<>( descendants );
    }

    public Set<Process> getAncestors() {
        return new HashSet<>( ancestors );
    }

    public void addInbound( Edge e ) {
        in.add( e );
        final Vertex from = e.getFrom();
        final Set<Process> fromAncestors = from.getAncestors();

        if ( from.getType() == Type.PROCESS ) {
            fromAncestors.add((Process) from);
        }

        this.addAncestor(fromAncestors);
        if ( from.getType() == Type.PROCESS ) {
            ((Process) from).addDescendant( this );
        }

        final Set<Process> descendantsCopy = this.getDescendants();
        fromAncestors.forEach( v -> {
            v.addDescendant( descendantsCopy );
            v.addDescendant( this );
        });
    }

    public void addOutbound( Edge e ) {
        out.add( e );
        final Vertex to = e.getTo();
        final Set<Process> toDescendants = to.getDescendants();

        if ( to.getType() == Type.PROCESS ) {
            toDescendants.add((Process) to);
        }

        this.addDescendant(toDescendants);
        if ( to.getType() == Type.PROCESS ) {
            ((Process)to).addAncestor( this );
        }

        final Set<Process> ancestorsCopy = this.getAncestors();
        toDescendants.forEach( v -> {
            v.addAncestor( ancestorsCopy );
            v.addAncestor( this );
        });

        final int rank = e.getTo().getRank();
        informNewDescendent( rank + 1 );

    }

    @Override
    int incRank( int rank ) {
        return rank + 1;
    }
}
