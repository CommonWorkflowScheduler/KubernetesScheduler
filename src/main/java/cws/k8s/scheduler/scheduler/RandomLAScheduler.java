package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.data.NodeDataTuple;
import cws.k8s.scheduler.scheduler.data.TaskData;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.util.FileAlignment;
import cws.k8s.scheduler.util.Tuple;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
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

    private Optional<NodeWithAlloc> selectNode( List<NodeDataTuple> matchingNodes, Task task ){
        if (matchingNodes.isEmpty()) return Optional.empty();
        else {
            return Optional.of( new LinkedList<>(matchingNodes).get(random.nextInt(matchingNodes.size())).getNode());
        }
    }

    @Override
    Tuple<NodeWithAlloc, FileAlignment> calculateBestNode(
            final TaskData taskData,
            CurrentlyCopying planedToCopy,
            Map<NodeWithAlloc, Requirements> availableByNode,
            Map<NodeWithAlloc, Integer> assignedPodsByNode
    ){
        final Optional<NodeWithAlloc> nodeWithAlloc = selectNode(taskData.getNodeDataTuples(), taskData.getTask());
        if (nodeWithAlloc.isEmpty()) {
            return null;
        }
        final NodeWithAlloc node = nodeWithAlloc.get();
        final CurrentlyCopyingOnNode currentlyCopying = getCurrentlyCopying().get(node.getNodeLocation());
        final CurrentlyCopyingOnNode currentlyPlanetToCopy = planedToCopy.get(node.getNodeLocation());
        final FileAlignment fileAlignment = getInputAlignment().getInputAlignment(
                taskData.getTask(),
                taskData.getMatchingFilesAndNodes().getInputsOfTask(),
                node,
                currentlyCopying,
                currentlyPlanetToCopy
        );
        return new Tuple<>( node, fileAlignment );
    }

    @Override
    TaskData calculateTaskData(
            final Task task,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ) {
        final MatchingFilesAndNodes matchingFilesAndNodes = getMatchingFilesAndNodes(task, availableByNode);
        if ( matchingFilesAndNodes == null || matchingFilesAndNodes.getNodes().isEmpty() ) {
            return null;
        }
        final List<NodeDataTuple> nodeDataTuples = matchingFilesAndNodes
                .getNodes()
                .parallelStream()
                .map(node -> new NodeDataTuple(node, 0 ) )
                .collect(Collectors.toList());
        //Do not set any weights, as it is randomly scheduled
        return new TaskData( 0, task, nodeDataTuples, matchingFilesAndNodes, 0, true );
    }


}
