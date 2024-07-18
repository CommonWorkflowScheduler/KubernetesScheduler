package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.taskinputs.PathFileLocationTriple;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;

import java.util.LinkedList;
import java.util.List;

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
