package fonda.scheduler.util;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NodeTaskAlignment {

    public final NodeWithAlloc node;
    public final Task task;

}
