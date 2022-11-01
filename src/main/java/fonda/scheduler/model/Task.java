package fonda.scheduler.model;

import fonda.scheduler.dag.DAG;
import fonda.scheduler.dag.Process;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.tracing.TraceRecord;
import fonda.scheduler.util.Batch;
import fonda.scheduler.util.Tuple;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;
import java.util.Map;

@Slf4j
public class Task {

    @Getter
    private final long submissionTime = System.currentTimeMillis();

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
    private PodWithAge pod = null;

    @Getter
    @Setter
    private NodeWithAlloc node = null;

    @Getter
    @Setter
    private Batch batch;

    @Getter
    @Setter
    private Map< String, Tuple<Task, Location>> copyingToNode;

    @Getter
    private final TraceRecord traceRecord = new TraceRecord();

    private long timeAddedToQueue;

    @Getter
    @Setter
    private boolean copiesDataToNode = false;

    public Task( TaskConfig config, DAG dag ) {
        this.config = config;
        this.process = dag.getByProcess( config.getTask() );
    }

    public String getWorkingDir(){
        return config.getWorkDir();
    }

    public boolean wasSuccessfullyExecuted(){
        return pod.getStatus().getContainerStatuses().get( 0 ).getState().getTerminated().getExitCode() == 0;
    }

    public void writeTrace(){
        try {
            final String tracePath = getWorkingDir() + '/' + ".command.scheduler.trace";
            traceRecord.writeRecord(tracePath);
        } catch ( Exception e ){
            log.warn( "Cannot write trace of task: " + this.getConfig().getName(), e );
        }
    }

    public void setPod(PodWithAge pod) {
        if( this.pod == null ) {
            timeAddedToQueue = System.currentTimeMillis();
        }
        this.pod = pod;
    }

    public void submitted(){
        traceRecord.setSchedulerTimeInQueue( System.currentTimeMillis() - timeAddedToQueue );
    }

    public String getOutLabel(){
        final OutLabel outLabel = config.getOutLabel();
        return outLabel == null ? null : outLabel.getLabel();
    }

    private long inputSize = -1;

    public long getInputSize(){
        synchronized ( this ) {
            if ( inputSize == -1 ) {
                //calculate
                inputSize = getConfig()
                        .getInputs()
                        .fileInputs
                        .parallelStream()
                        .mapToLong( input -> new File(input.value.sourceObj).length() )
                        .sum();
            }
        }
        //return cached value
        return inputSize;
    }

    @Override
    public String toString() {
        return "Task{" +
                "state=" + state +
                ", pod=" + (pod == null ? "--" : pod.getMetadata().getName()) +
                ", node='" + (node != null ? node.getNodeLocation().getIdentifier() : "--") + '\'' +
                ", workDir='" + getWorkingDir() + '\'' +
                '}';
    }
}
