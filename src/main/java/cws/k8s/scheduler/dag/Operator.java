package cws.k8s.scheduler.dag;

import java.util.HashSet;
import java.util.Set;

public class Operator extends NotProcess {

    Operator( String label, int uid ) {
        super(label,uid);
    }

    @Override
    public Type getType() {
        return Type.OPERATOR;
    }

    public Set<Process> getAncestors() {
        final HashSet<Process> results = new HashSet<>();
        if ( !in.isEmpty() ){
            for ( Edge edge : in ) {
                final Vertex from = edge.getFrom();
                if ( from.getType() == Type.PROCESS ) {
                    results.add((Process) from);
                }
                results.addAll( from.getAncestors() );
            }
        }
        return results;
    }

    public void addInbound( Edge e ) {
        in.add( e );
        final Vertex from = e.getFrom();
        final Set<Process> ancestors = from.getAncestors();
        if ( from.getType() == Type.PROCESS ) {
            ancestors.add((Process) from);
        }
        final Set<Process> descendants = this.getDescendants();
        ancestors.forEach( v -> v.addDescendant( descendants ) );
    }

}
