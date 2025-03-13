package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * This class is used to create a triple of a task, a node and a label count.
 * It is used to keep track which tasks are not on a node.
 */
@Getter
@RequiredArgsConstructor
public class DataMissing {

    private final Task task;
    private final NodeWithAlloc node;
    private final List<LabelCount> labelCounts;
    private final double score;

}
