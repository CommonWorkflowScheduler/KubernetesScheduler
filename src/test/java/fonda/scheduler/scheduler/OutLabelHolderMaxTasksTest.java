package fonda.scheduler.scheduler;

import fonda.scheduler.dag.DAG;
import fonda.scheduler.dag.Process;
import fonda.scheduler.dag.Vertex;
import fonda.scheduler.model.OutLabel;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.TaskConfig;
import fonda.scheduler.model.location.NodeLocation;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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
        final OutLabelHolderMaxTasks outLabelHolderMaxTasks = new OutLabelHolderMaxTasks();
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