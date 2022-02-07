package fonda.scheduler.dag;

import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class Vertex {

    private final String label;
    private final Type type;
    private final int uid;
    private final List<Edge> in = new LinkedList<>();
    private final List<Edge> out = new LinkedList<>();
    private final Set<Vertex> descendants;
    private final Set<Vertex> ancestors;

    private Vertex() {
        this( null, null, -1 );
    }

    /**
     * Just for testing
     * @param label
     * @param type
     * @param uid
     */
    Vertex(String label, Type type, int uid) {
        this.label = label;
        this.type = type;
        this.uid = uid;
        if ( type == Type.PROCESS ){
            descendants = new HashSet<>();
            ancestors = new HashSet<>();
        } else {
            descendants = null;
            ancestors = null;
        }
    }

    public void addInbound( Edge e ) {
        in.add( e );
        final Vertex from = e.getFrom();
        final Set<Vertex> ancestors = from.getAncestors();

        if ( from.type == Type.PROCESS ) ancestors.add( from );

        if ( type == Type.PROCESS ) {
            this.ancestors.addAll(ancestors);
            if ( from.type == Type.PROCESS ) from.descendants.add( this );
        }

        final Set<Vertex> descendants = this.getDescendants();
        ancestors.forEach( v -> {
            v.descendants.addAll( descendants );
            if ( type == Type.PROCESS ) v.descendants.add( this );
        });
    }

    public void addOutbound( Edge e ) {
        out.add( e );
        final Vertex to = e.getTo();
        final Set<Vertex> descendants = to.getDescendants();

        if ( to.type == Type.PROCESS ) descendants.add(to);

        if ( type == Type.PROCESS ) {
            this.descendants.addAll(descendants);
            if ( to.type == Type.PROCESS ) to.ancestors.add( this );
        }

        final Set<Vertex> ancestors = this.getAncestors();
        descendants.forEach( v -> {
            v.ancestors.addAll( ancestors );
            if ( type == Type.PROCESS ) v.ancestors.add( this );
        });
    }

    public Set<Vertex> getDescendants() {
        final HashSet<Vertex> results = new HashSet<>();
        if ( this.type == Type.PROCESS ) {
            results.addAll( descendants );
        } else if ( !out.isEmpty() ) {
            for ( Edge edge : out ) {
                final Vertex to = edge.getTo();
                if ( to.getType() == Type.PROCESS ) results.add( to );
                results.addAll( to.getDescendants() );
            }
        }
        return results;
    }

    public Set<Vertex> getAncestors() {
        final HashSet<Vertex> results = new HashSet<>();
        if ( this.type == Type.PROCESS ) {
            results.addAll( ancestors );
        } else if ( !in.isEmpty() && type != Type.ORIGIN ){
            for ( Edge edge : in ) {
                final Vertex from = edge.getFrom();
                if ( from.getType() == Type.PROCESS ) results.add( from );
                results.addAll( from.getAncestors() );
            }
        }
        return results;
    }

    private String collectionToString( Collection<Vertex> v ){
        return v.stream().map( Vertex::getUid ).sorted().map( c -> c.toString() )
                .collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "label='" + label + '\'' +
                ", type=" + type +
                ", uid=" + uid +
                ", descendants=" + collectionToString( getDescendants() ) +
                ", ancestors=" + collectionToString( getAncestors() ) +
                '}';
    }
}
