package fonda.scheduler.scheduler.data;

import fonda.scheduler.model.NodeWithAlloc;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

@Getter
@EqualsAndHashCode
public class NodeDataTuple implements Comparable<NodeDataTuple> {

    private final NodeWithAlloc node;
    private final long sizeInBytes;
    @Getter(AccessLevel.NONE)
    @Setter
    private double weight;

    public NodeDataTuple( NodeWithAlloc node, long sizeInBytes ) {
        this( node, sizeInBytes, 1.0 );
    }

    public NodeDataTuple(NodeWithAlloc node, long sizeInBytes, double weight ) {
        this.node = node;
        this.sizeInBytes = sizeInBytes;
        this.weight = weight;
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