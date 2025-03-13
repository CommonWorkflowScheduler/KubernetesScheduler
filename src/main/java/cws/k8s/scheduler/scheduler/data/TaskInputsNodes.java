package cws.k8s.scheduler.scheduler.data;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
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

    public long getTaskSize() {
        final long filesInSharedFS = task.getInputSize();
        return filesInSharedFS + inputsOfTask.calculateAvgSize();
    }
}