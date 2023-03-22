package cws.k8s.scheduler.scheduler.outlabel;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
abstract class InternalHolder {

    private double bestValue = Double.MIN_VALUE;
    private final Set<NodeLocation> currentlyBestNodes = new HashSet<>();
    private final Map<NodeLocation, Set<Task>> tasksByNode = new HashMap<>();

    public void addTask(Task task, NodeLocation node) {
        synchronized ( tasksByNode ) {
            final Set<Task> tasks = tasksByNode.computeIfAbsent(node, key -> new HashSet<>());
            tasks.add(task);
            final double calculatedValue = calculateValue(tasks);
            if (calculatedValue > bestValue) {
                bestValue = calculatedValue;
                currentlyBestNodes.clear();
            }
            if (calculatedValue >= bestValue) {
                currentlyBestNodes.add(node);
            }
        }
    }

    protected abstract double calculateValue( Set<Task> input );

    public Set<NodeLocation> getBestNode() {
        synchronized ( tasksByNode ){
            log.info("Best nodes: {}", currentlyBestNodes);
            return currentlyBestNodes;
        }
    }

}