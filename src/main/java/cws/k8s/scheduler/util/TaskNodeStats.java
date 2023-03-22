package cws.k8s.scheduler.util;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class TaskNodeStats {

    final long sizeRemaining;
    final long sizeCurrentlyCopying;
    final long sizeOnNode;

    public long getTaskSize() {
        return sizeRemaining + sizeCurrentlyCopying + sizeOnNode;
    }

    public boolean allOnNode() {
        return sizeRemaining == 0 && sizeCurrentlyCopying == 0;
    }

    public boolean allOnNodeOrCopying() {
        return sizeRemaining == 0;
    }

}