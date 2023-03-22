package cws.k8s.scheduler.scheduler.la2.ready2run;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.scheduler.data.TaskInputsNodes;
import cws.k8s.scheduler.util.LogCopyTask;
import cws.k8s.scheduler.util.NodeTaskLocalFilesAlignment;
import cws.k8s.scheduler.util.score.CalculateScore;

import java.util.List;
import java.util.Map;

public interface ReadyToRunToNode {

    void init( CalculateScore calculateScore );

    void setLogger( LogCopyTask logger );

    List<NodeTaskLocalFilesAlignment> createAlignmentForTasksWithAllDataOnNode(
            List<TaskInputsNodes> taskWithAllData,
            Map<NodeWithAlloc, Requirements> availableByNode
    );

}
