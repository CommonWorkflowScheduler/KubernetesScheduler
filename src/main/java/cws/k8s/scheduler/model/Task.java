package cws.k8s.scheduler.model;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import cws.k8s.scheduler.util.Batch;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
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

    /** Nextflow writes a trace file, when run with "-with-trace" on command 
     * line, or "trace.enabled = true" in the configuration file.
     * 
     * This method will get the peak resident set size (RSS) from there, and
     * return it in BigDecimal format.
     *  
     * @return The peak RSS value that this task has used
     */
    public java.math.BigDecimal getNfPeakRss() {
        final String nfTracePath = getWorkingDir() + '/' + ".command.trace";
    	try {
    		java.nio.file.Path path = java.nio.file.Paths.get(nfTracePath);
    		java.util.List<String> allLines = java.nio.file.Files.readAllLines(path);
    	    for (String a: allLines) {
    	    	if (a.startsWith("peak_rss")) {
    	    		java.math.BigDecimal peakRss = new java.math.BigDecimal(a.substring(9));
    	    		return peakRss.multiply(java.math.BigDecimal.valueOf(1024l));
    	    	}
    	    }
        } catch ( Exception e ){
            log.warn( "Cannot read nf .command.trace file in " + nfTracePath, e );
        }
    	return java.math.BigDecimal.ZERO;
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
}
