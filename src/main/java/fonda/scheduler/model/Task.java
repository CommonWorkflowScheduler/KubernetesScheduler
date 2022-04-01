package fonda.scheduler.model;

import fonda.scheduler.dag.DAG;
import fonda.scheduler.dag.Process;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.util.Batch;
import fonda.scheduler.util.Tuple;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;

public class Task {

    @Getter
    private final TaskConfig config;
    @Getter
    private final TaskState state = new TaskState();

    @Getter
    private final Process process;

    @Getter
    @Setter
    private List<LocationWrapper> inputFiles;

    @Getter
    @Setter
    private List< TaskInputFileLocationWrapper > copiedFiles;

    @Getter
    @Setter
    private PodWithAge pod = null;

    @Getter
    @Setter
    private NodeLocation node = null;

    @Getter
    @Setter
    private Batch batch;

    @Getter
    @Setter
    private HashMap< String, Tuple<Task, Location>> copyingToNode;

    public Task( TaskConfig config, DAG dag ) {
        this.config = config;
        this.process = dag.getByProcess( config.getTask() );
    }

    public String getWorkingDir(){
        if ( this.pod == null ) return null;
        return pod.getSpec().getContainers().get(0).getWorkingDir();
    }

    public boolean wasSuccessfullyExecuted(){
        return pod.getStatus().getContainerStatuses().get( 0 ).getState().getTerminated().getExitCode() == 0;
    }

    @Override
    public String toString() {
        return "Task{" +
                "state=" + state +
                ", pod=" + (pod == null ? "--" : pod.getMetadata().getName()) +
                ", node='" + (node != null ? node.getIdentifier() : "--") + '\'' +
                ", workDir='" + getWorkingDir() + '\'' +
                '}';
    }
}
