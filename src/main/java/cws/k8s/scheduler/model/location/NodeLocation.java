package cws.k8s.scheduler.model.location;

import io.fabric8.kubernetes.api.model.Node;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@RequiredArgsConstructor( access = AccessLevel.PRIVATE )
public class NodeLocation extends Location {

    private static final long serialVersionUID = 1L;

    private static final ConcurrentMap< String, NodeLocation > locationHolder = new ConcurrentHashMap<>();

    @Getter
    private final String identifier;

    public static NodeLocation getLocation( Node node ){
        return getLocation( node.getMetadata().getName() );
    }

    public static NodeLocation getLocation( String node ){
        if ( node == null ) {
            throw new IllegalArgumentException("Node cannot be null");
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeLocation)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        NodeLocation that = (NodeLocation) o;

        return getIdentifier() != null ? getIdentifier().equals(that.getIdentifier()) : that.getIdentifier() == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getIdentifier() != null ? getIdentifier().hashCode() : 0);
        return result;
    }
}
