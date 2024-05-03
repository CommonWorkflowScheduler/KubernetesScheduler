package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.Getter;

import java.util.List;

@Getter
public class TestTask extends Task {


    private final long inputSize;

    public TestTask() {
        this( 0, 0, 1 );
    }

    public TestTask( long inputSize ) {
        this( 0, 0, inputSize );
    }

    public TestTask( int numberFinishedTasks, int rank ) {
        this( numberFinishedTasks, rank, 1 );
    }

    public TestTask( int numberFinishedTasks, int rank, long inputSize ) {
        super( getTaskConfig(), getDag( numberFinishedTasks, rank ) );
        this.inputSize = inputSize;
    }

    private static DAG getDag( int numberFinishedTasks, int rank ){
        final DAG dag = new DAG();
        final Process a = new TestProcess( "a", 1, numberFinishedTasks, rank );
        List<Vertex> vertexList = List.of( a );
        dag.registerVertices( vertexList );
        return dag;
    }

    private static TaskConfig getTaskConfig(){
        return new TaskConfig( "a" );
    }

    @Override
    public String toString() {
        return "TestTask{" +
                "id=" + getId() +
                ", inputSize=" + inputSize +
                ", rank=" + getProcess().getRank() +
                ", numberFinishedTasks=" + getProcess().getSuccessfullyFinished() +
                '}';
    }
}