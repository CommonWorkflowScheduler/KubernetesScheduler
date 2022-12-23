package fonda.scheduler.util.copying;

import fonda.scheduler.model.location.NodeLocation;
import lombok.ToString;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ToString
public class CurrentlyCopying {

    private final Map<NodeLocation, CurrentlyCopyingOnNode> copyingToNode = new ConcurrentHashMap<>();

    public void add( NodeLocation nodeLocation, CurrentlyCopyingOnNode currentlyCopyingOnNode ) {
        if ( currentlyCopyingOnNode == null || currentlyCopyingOnNode.isEmpty() ) {
            return;
        }
        copyingToNode.compute( nodeLocation, ( node, currentlyCopying ) -> {
            if ( currentlyCopying == null ) {
                return currentlyCopyingOnNode;
            } else {
                currentlyCopying.add( currentlyCopyingOnNode );
                return currentlyCopying;
            }
        } );
    }

    public CurrentlyCopyingOnNode get( NodeLocation nodeLocation ) {
        return copyingToNode.computeIfAbsent( nodeLocation, node -> new CurrentlyCopyingOnNode() );
    }

    public void remove( NodeLocation nodeLocation, CurrentlyCopyingOnNode currentlyCopyingOnNode ) {
        if ( currentlyCopyingOnNode == null || currentlyCopyingOnNode.isEmpty() ) {
            return;
        }
        copyingToNode.compute( nodeLocation, ( node, currentlyCopying ) -> {
            if ( currentlyCopying == null ) {
                return null;
            } else {
                currentlyCopying.remove( currentlyCopyingOnNode );
                return currentlyCopying;
            }
        } );
    }



}
