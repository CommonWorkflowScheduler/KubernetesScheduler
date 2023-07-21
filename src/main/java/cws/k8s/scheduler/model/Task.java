package cws.k8s.scheduler.model;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.model.location.hierachy.HierarchyWrapper;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import cws.k8s.scheduler.util.Batch;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Slf4j
public class Task {

    private static final AtomicInteger idCounter = new AtomicInteger(0);

    @Getter
    private final int id = idCounter.getAndIncrement();

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
    private CurrentlyCopyingOnNode copyingToNode;

    @Getter
    private final TraceRecord traceRecord = new TraceRecord();

    private long timeAddedToQueue;

    @Getter
    @Setter
    private boolean copiesDataToNode = false;

    private final AtomicInteger copyTaskId = new AtomicInteger(0);

    private final HierarchyWrapper hierarchyWrapper;

    public Task( TaskConfig config, DAG dag ) {
        this( config, dag, null );
    }

    public Task( TaskConfig config, DAG dag, HierarchyWrapper hierarchyWrapper ) {
        this.config = config;
        this.process = dag.getByProcess( config.getTask() );
        this.hierarchyWrapper = hierarchyWrapper;
    }

    public int getCurrentCopyTaskId() {
        return copyTaskId.getAndIncrement();
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

    /**
     * Calculates the size of all input files in bytes in the shared filesystem.
     * @return The sum of all input files in bytes.
     */
    public long getInputSize(){
        synchronized ( this ) {
            if ( inputSize == -1 ) {
                //calculate
                Stream<InputParam<FileHolder>> inputParamStream = getConfig()
                        .getInputs()
                        .fileInputs
                        .parallelStream();
                //If LA Scheduling, filter out files that are not in sharedFS
                if ( hierarchyWrapper != null ) {
                    inputParamStream = inputParamStream.filter( x -> {
                        final Path path = Path.of( x.value.sourceObj );
                        return !hierarchyWrapper.isInScope( path );
                    } );
                }
                inputSize = inputParamStream
                        .mapToLong( input -> new File(input.value.sourceObj).length() )
                        .sum();
            }
        }
        //return cached value
        return inputSize;
    }

    public Requirements getRequest() {
        return pod.getRequest();
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
