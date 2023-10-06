package cws.k8s.scheduler.scheduler.la2;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.util.TaskNodeStats;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor
public class TaskStat implements Comparable<TaskStat> {

    @Getter
    private final Task task;
    @Getter
    private final TaskInputs inputsOfTask;
    private TaskStatComparator comparator;
    private final List<NodeAndStatWrapper> taskStats = new ArrayList<>();
    @Getter
    private int completeOnNodes = 0;
    @Getter
    private int copyingToNodes = 0;
    private boolean finished = false;
    private int indexToCompare = 0;

    @Getter
    private boolean copyToNodeWithAvailableResources = false;

    public boolean canStartSomewhere() {
        return !taskStats.isEmpty();
    }

    /**
     * @return number is the number of nodes, that are better for this task depending on the comparator.
     */
    public int getCurrentIndex() {
        return indexToCompare;
    }

    /**
     * @return number of nodes that have already all data or are copying remaining data to this node.
     */
    public int dataOnNodes() {
        return completeOnNodes + copyingToNodes;
    }

    public void setComparator( TaskStatComparator comparator ) {
        this.comparator = comparator;
        this.indexToCompare = 0;
        finish();
    }

    public void copyToNodeWithAvailableResources() {
        this.copyToNodeWithAvailableResources = true;
    }

    public boolean missingDataOnAnyNode() {
        return !taskStats.isEmpty();
    }

    /**
     * Call this to sort the taskStats list
     */
    private void finish() {
        finished = true;
        taskStats.sort( comparator.getComparator() );
    }

    /**
     * Increases the index to compare.
     * False if no other opportunity exists
     * @return
     */
    public boolean increaseIndexToCompare() {
        indexToCompare++;
        return indexToCompare < taskStats.size();
    }

    private final AtomicInteger canStartAfterCopying = new AtomicInteger( 0 );


    /**
     * Will only be added to the list if there is missing data on the node.
     * @param node
     * @param taskNodeStats
     */
    public void add( NodeWithAlloc node, TaskNodeStats taskNodeStats ) {
        assert !finished;
        if ( taskNodeStats.allOnNode() ) {
            this.completeOnNodes++;
        } else if ( taskNodeStats.allOnNodeOrCopying() ) {
            this.copyingToNodes++;
        } else {
            //Only add in the case that not all data is already on the node
            this.taskStats.add( new NodeAndStatWrapper( node, taskNodeStats, task ) );
        }
    }

    /**
     * Mark this task as it is currently copying to a node with enough available resources.
     */
    public void canStartAfterCopying() {
        this.canStartAfterCopying.incrementAndGet();
    }

    public int getCanStartAfterCopying() {
        return this.canStartAfterCopying.get();
    }

    public NodeAndStatWrapper getBestStats() {
        assert finished;
        assert indexToCompare < taskStats.size();
        return taskStats.get( indexToCompare );
    }

    @Override
    public int compareTo( @NotNull TaskStat o ) {
        if ( indexToCompare >= taskStats.size() ) {
            throw new IllegalStateException( "Cannot compare task " + task.getConfig().getName() + " size = " + taskStats.size() + " indexToCompare = " + indexToCompare);
        }
        if ( o.indexToCompare >= o.taskStats.size() ) {
            throw new IllegalStateException( "Cannot compare task " + o.task.getConfig().getName() + " size = " + o.taskStats.size() + " indexToCompare = " + o.indexToCompare );
        }
        return comparator.compare( this, o );
    }

    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class NodeAndStatWrapper {
        private final NodeWithAlloc node;
        private final TaskNodeStats taskNodeStats;
        private final Task task;
    }


}
