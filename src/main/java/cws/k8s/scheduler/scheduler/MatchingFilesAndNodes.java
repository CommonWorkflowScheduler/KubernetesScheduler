package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Set;

@Getter
@RequiredArgsConstructor
public class MatchingFilesAndNodes {

    private final Set<NodeWithAlloc> nodes;
    private final TaskInputs inputsOfTask;

}
