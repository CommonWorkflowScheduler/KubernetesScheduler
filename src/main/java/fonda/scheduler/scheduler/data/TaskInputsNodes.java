package fonda.scheduler.scheduler.data;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.taskinputs.TaskInputs;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * This class is used to store the data of a task, the task, and the nodes that contain all data for this task.
 */
@Getter
@RequiredArgsConstructor
public class TaskInputsNodes {
    private final Task task;
    private final List<NodeWithAlloc> nodesWithAllData;
    private final TaskInputs inputsOfTask;
}