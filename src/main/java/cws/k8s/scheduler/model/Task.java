package cws.k8s.scheduler.model;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import cws.k8s.scheduler.util.Batch;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

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
    private PodWithAge pod = null;

    @Getter
    @Setter
    private NodeWithAlloc node = null;

    @Getter
    @Setter
    private Batch batch;

    @Getter
    private final TraceRecord traceRecord = new TraceRecord();

    private long timeAddedToQueue;

    @Getter
    private TaskMetrics taskMetrics = null;

    private final Requirements oldRequirements;

    @Getter
    private Requirements planedRequirements;

    public Task( TaskConfig config, DAG dag ) {
        this.config = config;
        oldRequirements = new Requirements( BigDecimal.valueOf(config.getCpus()), BigDecimal.valueOf(config.getMemoryInBytes()) );
        planedRequirements = new Requirements( BigDecimal.valueOf(config.getCpus()), BigDecimal.valueOf(config.getMemoryInBytes()) );
        this.process = dag.getByProcess( config.getTask() );
    }

    public synchronized void setTaskMetrics( TaskMetrics taskMetrics ){
        if ( this.taskMetrics != null ){
            throw new IllegalArgumentException( "TaskMetrics already set for task: " + this.getConfig().getName() );
        }
        log.info( "Setting taskMetrics for task: " + this.getConfig().getName() );
        this.taskMetrics = taskMetrics;
    }

    public String getWorkingDir(){
        return config.getWorkDir();
    }

    public Integer getExitCode(){
        return pod.getStatus().getContainerStatuses().get( 0 ).getState().getTerminated().getExitCode();
    }

    public boolean wasSuccessfullyExecuted(){
        return getExitCode() == 0;
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
                ", workDir='" + getWorkingDir() + '\'' +
                '}';
    }

    public long getNewMemoryRequest(){
        return getPlanedRequirements().getRam().longValue();
    }

    public BigDecimal getOriginalMemoryRequest(){
        return oldRequirements.getRam();
    }

    public void setPlannedMemoryInBytes( long memory ){
        planedRequirements = new Requirements( planedRequirements.getCpu(), BigDecimal.valueOf(memory) );
    }

    public void setPlanedCpuInCores( double cpu ){
        planedRequirements = new Requirements( BigDecimal.valueOf(cpu), planedRequirements.getRam() );
    }

    public boolean requirementsChanged(){
        return !oldRequirements.equals( planedRequirements );
    }

}
