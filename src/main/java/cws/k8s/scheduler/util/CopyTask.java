package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.TaskInputFileLocationWrapper;
import cws.k8s.scheduler.scheduler.schedulingstrategy.Inputs;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Getter
@RequiredArgsConstructor
public class CopyTask {

    @Getter(AccessLevel.NONE)
    private static final AtomicLong copyTaskId = new AtomicLong(0);
    private final long id = copyTaskId.getAndIncrement();
    private final Inputs inputs;
    private final LinkedList<TaskInputFileLocationWrapper> inputFiles;
    private final CurrentlyCopyingOnNode filesForCurrentNode;
    private final Task task;

    @Setter
    private List<LocationWrapper> allLocationWrapper;

    @Setter
    private NodeLocation nodeLocation;

}
