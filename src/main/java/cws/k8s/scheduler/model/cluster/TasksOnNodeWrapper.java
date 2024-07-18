package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.NodeWithAlloc;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
@AllArgsConstructor
public class TasksOnNodeWrapper implements Comparable<TasksOnNodeWrapper> {

    private final NodeWithAlloc node;
    private int share = 0;

    @Override
    public int compareTo( @NotNull TasksOnNodeWrapper o ) {
        return Integer.compare( o.share, share );
    }

    public void addRunningTask() {
        share++;
    }
}
