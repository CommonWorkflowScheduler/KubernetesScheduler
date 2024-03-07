package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;

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

    public TestTask( double x, double y) {
        super( tc, dag );
        this.x = x;
        this.y = y;
    }

    public TestTask() {
        this( 0, 0 );
    }

}