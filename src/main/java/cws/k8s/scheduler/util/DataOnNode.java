package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataOnNode {


    private final Map<NodeWithAlloc,Double> dataOnNode = new HashMap<>();

    @Getter
    private final long overAllData;
    @Getter
    private final Task task;
    @Getter
    private int nodesWithAllData;
    @Getter
    private final TaskInputs inputsOfTask;

    public DataOnNode( Task task, TaskInputs inputsOfTask ) {
        this.task = task;
        this.inputsOfTask = inputsOfTask;
        this.overAllData = inputsOfTask.calculateAvgSize();
    }

    public void allData( NodeWithAlloc node ){
        nodesWithAllData++;
        addData( node, 1 );
    }

    public void addData( NodeWithAlloc node, double part ){
        dataOnNode.put( node, part );
    }

    public double getPart( NodeWithAlloc node ){
        return dataOnNode.get( node );
    }

    public Set<NodeWithAlloc> getNodes() {
        return dataOnNode.keySet();
    }

}
