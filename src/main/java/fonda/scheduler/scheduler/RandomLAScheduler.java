package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.scheduler.data.NodeDataTuple;
import fonda.scheduler.scheduler.data.TaskData;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class RandomLAScheduler extends LocationAwareScheduler {

    private final Random random = new Random();

    public RandomLAScheduler(
            String name,
            KubernetesClient client,
            String namespace,
            SchedulerConfig config,
            InputAlignment inputAlignment
    ) {
        super(name, client, namespace, config, inputAlignment);
    }

    private Optional<NodeWithAlloc> selectNode( Set<NodeWithAlloc> matchingNodes, Task task ){
        return matchingNodes.isEmpty()
                ? Optional.empty()
                : Optional.of( new LinkedList<>(matchingNodes).get(random.nextInt(matchingNodes.size())));
    }

    @Override
    Tuple<NodeWithAlloc, FileAlignment> calculateBestNode(
            final TaskData taskData,
            final Set<NodeWithAlloc> matchingNodesForTask
    ){
        final Optional<NodeWithAlloc> nodeWithAlloc = selectNode(matchingNodesForTask, taskData.getTask());
        if (nodeWithAlloc.isEmpty()) return null;
        final NodeWithAlloc node = nodeWithAlloc.get();
        final Map<String, Tuple<Task, Location>> currentlyCopying = getCopyingToNode().get(node.getNodeLocation());
        final FileAlignment fileAlignment = getInputAlignment().getInputAlignment(
                taskData.getTask(),
                taskData.getMatchingFilesAndNodes().getInputsOfTask(),
                node,
                currentlyCopying
        );
        return new Tuple<>( node, fileAlignment );
    }

    @Override
    TaskData calculateTaskData(
            final Task task,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ) {
        final MatchingFilesAndNodes matchingFilesAndNodes = getMatchingFilesAndNodes(task, availableByNode);
        if ( matchingFilesAndNodes == null || matchingFilesAndNodes.getNodes().isEmpty() ) return null;
        final List<NodeDataTuple> nodeDataTuples = matchingFilesAndNodes
                .getNodes()
                .parallelStream()
                .map(node -> new NodeDataTuple(node, 0 ) )
                .collect(Collectors.toList());
        return new TaskData( 0, task, nodeDataTuples, matchingFilesAndNodes );
    }


}
