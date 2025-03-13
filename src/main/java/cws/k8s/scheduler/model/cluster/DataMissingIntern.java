package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * This is a temporary class to group a task, a node and a label count.
 */
@RequiredArgsConstructor
@Getter
public class DataMissingIntern {

    private final Task task;
    private final NodeWithAlloc node;
    private final LabelCount labelCount;

}
