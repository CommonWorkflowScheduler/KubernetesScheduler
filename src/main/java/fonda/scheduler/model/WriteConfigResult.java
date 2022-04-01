package fonda.scheduler.model;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.util.Tuple;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class WriteConfigResult {

    private final List<TaskInputFileLocationWrapper> inputFiles;
    private final Map<String, Task > waitForTask;
    private final HashMap< String, Tuple<Task, Location>> copyingToNode;

    public WriteConfigResult(
            List<TaskInputFileLocationWrapper> inputFiles,
            Map<String, Task> waitForTask,
            HashMap<String, Tuple<Task, Location>> copyingToNode
    ) {
        this.inputFiles = inputFiles;
        this.waitForTask = waitForTask;
        this.copyingToNode = copyingToNode;
    }
}
