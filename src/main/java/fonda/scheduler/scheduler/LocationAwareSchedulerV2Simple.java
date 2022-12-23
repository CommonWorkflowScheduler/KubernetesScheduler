package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.scheduler.LocationAwareSchedulerV2;
import fonda.scheduler.scheduler.data.TaskInputsNodes;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.util.NodeTaskLocalFilesAlignment;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class LocationAwareSchedulerV2Simple extends LocationAwareSchedulerV2 {

    public LocationAwareSchedulerV2Simple( String name, KubernetesClient client, String namespace, SchedulerConfig config, InputAlignment inputAlignment ) {
        super( name, client, namespace, config, inputAlignment );
    }

    /**
     * Select the first node that fits the requirements.
     * @param taskWithAllData
     * @param availableByNode
     * @return
     */
    @Override
    protected List<NodeTaskLocalFilesAlignment> createAlignmentForTasksWithAllDataOnNode(
            List<TaskInputsNodes> taskWithAllData,
            Map<NodeWithAlloc, Requirements> availableByNode
    ) {
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
        return alignment;
    }
}
