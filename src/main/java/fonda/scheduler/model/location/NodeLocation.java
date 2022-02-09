package fonda.scheduler.model.location;

import io.fabric8.kubernetes.api.model.Node;
import lombok.Getter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NodeLocation extends Location {

    static final private ConcurrentMap< String, NodeLocation > locationHolder = new ConcurrentHashMap<>();

    @Getter
    private final String identifier;

    private NodeLocation(String identifier) {
        this.identifier = identifier;
    }

    public static NodeLocation getLocation( Node node ){
        return getLocation( node.getMetadata().getName() );
    }

    public static NodeLocation getLocation( String node ){
        if ( node == null ) throw new IllegalArgumentException("Node cannot be null");
        final NodeLocation nodeLocation = locationHolder.get(node);
        if ( nodeLocation == null ){
            locationHolder.putIfAbsent( node, new NodeLocation( node ) );
            return locationHolder.get( node );
        }
        return nodeLocation;
    }

    @Override
    public LocationType getType() {
        return LocationType.NODE;
    }

    @Override
    public String toString() {
        return "Node(" + identifier + ")";
    }

}
