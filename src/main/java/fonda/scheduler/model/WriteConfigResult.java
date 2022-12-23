package fonda.scheduler.model;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.util.Tuple;
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
public class WriteConfigResult {

    private final List<TaskInputFileLocationWrapper> inputFiles;
    private final Map<String, Task > waitForTask;
    private final CurrentlyCopyingOnNode copyingToNode;

    private boolean copyDataToNode;

    private final boolean wroteConfig;

    public WriteConfigResult(
            List<TaskInputFileLocationWrapper> inputFiles,
            Map<String, Task> waitForTask,
            CurrentlyCopyingOnNode copyingToNode,
            boolean wroteConfig,
            boolean copyDataToNode
    ) {
        this.inputFiles = inputFiles;
        this.waitForTask = waitForTask;
        this.copyingToNode = copyingToNode;
        this.wroteConfig = wroteConfig;
        this.copyDataToNode = copyDataToNode;
    }
}
