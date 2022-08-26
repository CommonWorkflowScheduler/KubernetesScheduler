package fonda.scheduler.model;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.util.Tuple;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
public class WriteConfigResult {

    private final List<TaskInputFileLocationWrapper> inputFiles;
    private final Map<String, Task > waitForTask;
    private final Map< String, Tuple<Task, Location>> copyingToNode;

    private final boolean wroteConfig;

    public WriteConfigResult(
            List<TaskInputFileLocationWrapper> inputFiles,
            Map<String, Task> waitForTask,
            Map<String, Tuple<Task, Location>> copyingToNode,
            boolean wroteConfig
    ) {
        this.inputFiles = inputFiles;
        this.waitForTask = waitForTask;
        this.copyingToNode = copyingToNode;
        this.wroteConfig = wroteConfig;
    }
}
