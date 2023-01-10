package fonda.scheduler.scheduler;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.taskinputs.TaskInputs;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Set;

@Getter
@RequiredArgsConstructor
public class MatchingFilesAndNodes {

    private final Set<NodeWithAlloc> nodes;
    private final TaskInputs inputsOfTask;

}
