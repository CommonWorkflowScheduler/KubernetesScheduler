package fonda.scheduler.scheduler.internal;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.scheduler.data.TaskInputsNodes;
import fonda.scheduler.util.LogCopyTask;
import fonda.scheduler.util.NodeTaskLocalFilesAlignment;
import fonda.scheduler.util.score.CalculateScore;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class RandomReadyToRunToNode implements ReadyToRunToNode {


    @Setter
    private LogCopyTask logger;


    @Override
    public void init( CalculateScore calculateScore ) {}

    /**
     * Select the first node that fits the requirements.
     * @param taskWithAllData
     * @param availableByNode
     * @return
     */
    @Override
    public List<NodeTaskLocalFilesAlignment> createAlignmentForTasksWithAllDataOnNode(
            List<TaskInputsNodes> taskWithAllData,
            Map<NodeWithAlloc, Requirements> availableByNode
    ) {
        long start = System.currentTimeMillis();
        final List<NodeTaskLocalFilesAlignment> alignment = new LinkedList<>();
        final Iterator<TaskInputsNodes> iterator = taskWithAllData.iterator();
        while( iterator.hasNext() ) {
            final TaskInputsNodes taskInputsNodes = iterator.next();
            final Requirements taskRequest = taskInputsNodes.getTask().getPod().getRequest();
            for ( NodeWithAlloc nodeWithAll : taskInputsNodes.getNodesWithAllData() ) {
                final Requirements availableOnNode = availableByNode.get( nodeWithAll );
                if ( availableOnNode != null &&
                        availableOnNode.higherOrEquals( taskRequest ) ) {
                    alignment.add(
                            new NodeTaskLocalFilesAlignment(
                                    nodeWithAll,
                                    taskInputsNodes.getTask(),
                                    taskInputsNodes.getInputsOfTask().getSymlinks(),
                                    taskInputsNodes.getInputsOfTask().allLocationWrapperOnLocation( nodeWithAll.getNodeLocation() )
                            )
                    );
                    availableOnNode.subFromThis( taskRequest );
                    iterator.remove();
                    break;
                }
            }
        }
        final String message = "Solved in " + (System.currentTimeMillis() - start) + "ms ( " + taskWithAllData.size() + " vars )";
        log.info( message );
        logger.log( message );
        return alignment;
    }

}
