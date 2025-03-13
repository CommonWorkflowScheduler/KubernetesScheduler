package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.taskinputs.PathFileLocationTriple;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;

import java.util.LinkedList;
import java.util.List;

/**
 * Fake task inputs are used to create {@link CopyTask}s that are not real tasks but are used to copy data between nodes.
 * This task can only be run on the node that is included in the constructor.
 */
public class FakeTaskInputs extends TaskInputs {

    private final Location includedNode;

    public FakeTaskInputs( List<PathFileLocationTriple> files, Location includedNode ) {
        super( new LinkedList<>(), files, null );
        this.includedNode = includedNode;
    }

    @Override
    public boolean canRunOnLoc( Location loc ) {
        return includedNode == loc;
    }

    @Override
    public boolean hasExcludedNodes() {
        return true;
    }
}
