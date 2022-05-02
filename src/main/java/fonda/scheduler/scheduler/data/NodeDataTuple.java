package fonda.scheduler.scheduler.data;

import fonda.scheduler.model.NodeWithAlloc;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

@Getter
public
class NodeDataTuple implements Comparable<NodeDataTuple> {

        private final NodeWithAlloc node;
        private final long sizeInBytes;

        public NodeDataTuple(NodeWithAlloc node, long sizeInBytes) {
            this.node = node;
            this.sizeInBytes = sizeInBytes;
        }

        @Override
        public int compareTo(@NotNull NodeDataTuple o) {
            return Long.compare( sizeInBytes, o.sizeInBytes);
        }
}