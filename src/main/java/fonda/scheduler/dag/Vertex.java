package fonda.scheduler.dag;

import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public abstract class Vertex {

    private final String label;
    private final int uid;
    final List<Edge> in = new LinkedList<>();
    final List<Edge> out = new LinkedList<>();

    Vertex( String label, int uid ) {
        this.label = label;
        this.uid = uid;
    }

    public abstract Type getType();

    public abstract void addInbound( Edge e );

    public abstract void addOutbound( Edge e );

    public abstract Set<Process> getDescendants();

    public abstract Set<Process> getAncestors();

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
}
