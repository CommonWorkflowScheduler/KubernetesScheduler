package cws.k8s.scheduler.scheduler.data;

import cws.k8s.scheduler.model.NodeWithAlloc;
import lombok.*;
import org.jetbrains.annotations.NotNull;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class NodeDataTuple implements Comparable<NodeDataTuple> {

    private final NodeWithAlloc node;
    private final long sizeInBytes;
    @Getter(AccessLevel.NONE)
    @Setter
    private double weight;

    public NodeDataTuple( NodeWithAlloc node, long sizeInBytes ) {
        this( node, sizeInBytes, 1.0 );
    }

    /**
     * @return reduce the worth, if this is not the outLabelNode
     */
    public double getWorth() {
        return getSizeInBytes() / weight;
    }

    @Override
    public int compareTo(@NotNull NodeDataTuple o) {
        return Double.compare( getWorth(), o.getWorth() );
    }
}