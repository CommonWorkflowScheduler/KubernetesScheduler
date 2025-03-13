package cws.k8s.scheduler.model;

import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@AllArgsConstructor
public class WriteConfigResult {

    private final List<TaskInputFileLocationWrapper> inputFiles;
    private final Map<String, Task > waitForTask;
    private final CurrentlyCopyingOnNode copyingToNode;
    private final boolean wroteConfig;
    private boolean copyDataToNode;

}
