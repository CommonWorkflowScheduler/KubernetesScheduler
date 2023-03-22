package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NodeTaskAlignment {

    public final NodeWithAlloc node;
    public final Task task;

}
