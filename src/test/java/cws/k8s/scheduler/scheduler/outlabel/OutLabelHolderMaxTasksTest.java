package cws.k8s.scheduler.scheduler.outlabel;

import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.OutLabel;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

class OutLabelHolderMaxTasksTest {

    private class TaskConfigMock extends TaskConfig {

        public TaskConfigMock(String task) {
            super(task);
        }

        @Override
        public OutLabel getOutLabel() {
            return new OutLabel( "a", 2.0 );
        }
    }

    @Test
    void determineBestNode() {
        final HolderMaxTasks outLabelHolderMaxTasks = new HolderMaxTasks();
        DAG dag = new DAG();
        List<Vertex> vertexList = new LinkedList<>();
        vertexList.add(new Process("processA", 1));
        dag.registerVertices(vertexList);
        final TaskConfig taskConfig = new TaskConfigMock("processA");
        final NodeLocation nodeA = NodeLocation.getLocation("nodeA");
        final NodeLocation nodeB = NodeLocation.getLocation("nodeB");
        outLabelHolderMaxTasks.scheduleTaskOnNode( new Task( taskConfig, dag ), nodeA);
        assertEquals( nodeA, outLabelHolderMaxTasks.getNodeForLabel( "a" ) );
        outLabelHolderMaxTasks.scheduleTaskOnNode( new Task( taskConfig, dag ), nodeB);
        outLabelHolderMaxTasks.scheduleTaskOnNode( new Task( taskConfig, dag ), nodeB);
        assertEquals( nodeB, outLabelHolderMaxTasks.getNodeForLabel( "a" ) );
    }

}