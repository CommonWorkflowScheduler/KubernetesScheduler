package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.Getter;

import java.math.BigDecimal;
import java.util.List;

public class TestTask extends Task {

    static final DAG dag = new DAG();
    static final TaskConfig tc = new TaskConfig( "a" );

    static {
        final Process a = new Process("a", 1);
        List<Vertex> vertexList = List.of( a );
        dag.registerVertices( vertexList );
    }

    public final double x;
    public final double y;

    private BigDecimal memoryRequest;
    @Getter
    private long inputSize;

    private long planedMemory = 0;

    public TestTask( double x, double y) {
        super( tc, dag );
        this.x = x;
        this.y = y;
    }

    public TestTask( long memoryRequest, long inputsize ) {
        this( 0d, 0d );
        this.memoryRequest = BigDecimal.valueOf( memoryRequest );
        planedMemory = memoryRequest;
        this.inputSize = inputsize;
    }

    @Override
    public BigDecimal getOriginalMemoryRequest() {
        return memoryRequest;
    }

    public void setPlannedMemoryInBytes( long memory ){
        this.planedMemory = memory;
    }

    public long getNewMemoryRequest(){
        return planedMemory;
    }

}