package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
public class DataMissing {

    private final Task task;
    private final NodeWithAlloc node;
    private final LabelCount labelCount;

}
