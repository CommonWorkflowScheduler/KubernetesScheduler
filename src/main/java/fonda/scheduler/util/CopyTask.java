package fonda.scheduler.util;

import fonda.scheduler.model.Task;
import fonda.scheduler.model.TaskInputFileLocationWrapper;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.scheduler.schedulingstrategy.Inputs;
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public class CopyTask {

    @Getter(AccessLevel.NONE)
    private static AtomicLong copyTaskId = new AtomicLong(0);
    private long id = copyTaskId.getAndIncrement();
    private final Inputs inputs;
    private final LinkedList<TaskInputFileLocationWrapper> inputFiles;
    private final CurrentlyCopyingOnNode filesForCurrentNode;

    @Setter
    private List<LocationWrapper> allLocationWrapper;

    @Setter
    private NodeLocation nodeLocation;

    public CopyTask( Inputs inputs, LinkedList<TaskInputFileLocationWrapper> inputFiles, CurrentlyCopyingOnNode filesForCurrentNode ) {
        this.inputs = inputs;
        this.inputFiles = inputFiles;
        this.filesForCurrentNode = filesForCurrentNode;
    }
}
