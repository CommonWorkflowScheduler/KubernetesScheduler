package fonda.scheduler.dag;

import lombok.Getter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
public abstract class Vertex {

    private final String label;
    private final int uid;
    final Set<Edge> in = new HashSet<>();
    final Set<Edge> out = new HashSet<>();

    Vertex( String label, int uid ) {
        this.label = label;
        this.uid = uid;
    }

    public abstract Type getType();

    public abstract void addInbound( Edge e );

    public void removeInbound( Edge e ) {
        final boolean remove = in.remove(e);
        if ( !remove ) {
            throw new IllegalStateException("Edge " + e + " not found in " + this);
        }
        removeInboundIntern( e );
    }

    public void removeInboundIntern( Edge e ) {
        final boolean remove = e.getFrom().out.remove(e);
        if ( !remove ) {
            throw new IllegalStateException("Edge " + e + " not found in " + this);
        }
        final Set<Process> ancestors = e.getFrom().getAncestors();
        if ( e.getFrom().getType() == Type.PROCESS ) {
            ancestors.add((Process) e.getFrom());
        }
        for (Edge edge : in) {
            if ( edge != e ) {
                ancestors.removeAll( edge.getFrom().getAncestors() );
            }
        }
        this.removeAncestor( e, ancestors );
        for (Edge edge : out) {
            edge.getTo().removeAncestor( edge, ancestors );
        }
        final Set<Process> descendants = getDescendants();
        if ( getType() == Type.PROCESS ) {
            descendants.add((Process) this);
        }
        e.getFrom().removeDescendant( e, descendants );
    }

    public void removeOutbound( Edge e ) {
        final boolean remove = out.remove(e);
        if ( !remove ) {
            throw new IllegalStateException("Edge " + e + " not found in " + this);
        }
        removeOutboundIntern( e );
    }

    public void removeOutboundIntern( Edge e ) {
        e.getTo().removeInbound( e );
    }

    public void deleteItself(){
        for (Edge edge : in) {
            removeInboundIntern( edge );
        }
        for (Edge edge : out) {
            removeOutboundIntern( edge );
        }
    }

    public abstract void addOutbound( Edge e );

    public abstract Set<Process> getDescendants();

    abstract void removeDescendant( Edge edge, Collection<Process> p );

    public abstract Set<Process> getAncestors();

    abstract void removeAncestor( Edge edge, Collection<Process> p );

    private String collectionToString( Collection<Process> v ){
        return v.stream().map( Process::getUid ).sorted().map( Object::toString )
                .collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return getType() + "{" +
                (label != null ? ("label='" + label + "', '") : "") +
                "uid=" + uid +
                ", descendants=" + collectionToString( getDescendants() ) +
                ", ancestors=" + collectionToString( getAncestors() ) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Vertex)) return false;

        Vertex vertex = (Vertex) o;

        return getUid() == vertex.getUid();
    }

    @Override
    public int hashCode() {
        return getUid();
    }
}
