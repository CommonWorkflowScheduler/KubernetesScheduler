package fonda.scheduler.scheduler;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.taskinputs.TaskInputs;
import lombok.Getter;

import java.util.Set;

@Getter
public class MatchingFilesAndNodes {

    private final Set<NodeWithAlloc> nodes;
    private final TaskInputs inputsOfTask;

    public MatchingFilesAndNodes( Set<NodeWithAlloc> nodes, TaskInputs inputsOfTask ) {
        this.nodes = nodes;
        this.inputsOfTask = inputsOfTask;
    }

}
