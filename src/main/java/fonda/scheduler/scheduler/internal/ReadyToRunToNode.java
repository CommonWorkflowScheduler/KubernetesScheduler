package fonda.scheduler.scheduler.internal;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.scheduler.data.TaskInputsNodes;
import fonda.scheduler.util.LogCopyTask;
import fonda.scheduler.util.NodeTaskLocalFilesAlignment;
import fonda.scheduler.util.score.CalculateScore;

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
