package fonda.scheduler.dag;

import java.util.HashSet;
import java.util.Set;

public class Process extends Vertex {


    final Set<Process> descendants;
    final Set<Process> ancestors;

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

        this.ancestors.addAll(fromAncestors);
        if ( from.getType() == Type.PROCESS ) {
            ((Process) from).descendants.add( this );
        }

        final Set<Process> descendantsCopy = this.getDescendants();
        fromAncestors.forEach( v -> {
            v.descendants.addAll( descendantsCopy );
            v.descendants.add( this );
        });
    }

    public void addOutbound( Edge e ) {
        out.add( e );
        final Vertex to = e.getTo();
        final Set<Process> toDescendants = to.getDescendants();

        if ( to.getType() == Type.PROCESS ) {
            toDescendants.add((Process) to);
        }

        this.descendants.addAll(toDescendants);
        if ( to.getType() == Type.PROCESS ) {
            ((Process)to).ancestors.add( this );
        }

        final Set<Process> ancestorsCopy = this.getAncestors();
        toDescendants.forEach( v -> {
            v.ancestors.addAll( ancestorsCopy );
            v.ancestors.add( this );
        });
    }

}
