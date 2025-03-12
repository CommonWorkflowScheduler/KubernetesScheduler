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

    public double getWorth() {
        return getSizeInBytes();
    }

    @Override
    public int compareTo(@NotNull NodeDataTuple o) {
        return Double.compare( getWorth(), o.getWorth() );
    }
}