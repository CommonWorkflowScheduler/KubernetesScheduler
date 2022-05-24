package fonda.scheduler.scheduler.data;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Requirements;
import fonda.scheduler.model.Task;
import fonda.scheduler.scheduler.MatchingFilesAndNodes;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Getter
@EqualsAndHashCode
public class TaskData implements Comparable<TaskData> {

    private final double size;

    private final Task task;

    private final List<NodeDataTuple> nodeDataTuples;

    private final MatchingFilesAndNodes matchingFilesAndNodes;

    private long timeInNs = 0;

    private final double antiStarvingFactor;

    @Getter(AccessLevel.NONE)
    private NodeDataTuple calulatedFor = null;

    private double value = 0;

    public TaskData(
            double size,
            Task task,
            List<NodeDataTuple> nodeDataTuples,
            MatchingFilesAndNodes matchingFilesAndNodes,
            double antiStarvingFactor) {
        this.size = size;
        this.task = task;
        this.nodeDataTuples = nodeDataTuples;
        this.matchingFilesAndNodes = matchingFilesAndNodes;
        this.antiStarvingFactor = antiStarvingFactor;
        calc();
    }

    /**
     * Recalculate the value. New value is smaller or equal old value
     * @param availableByNode
     * @return if the value has changed
     */
    public boolean calculate( Map<NodeWithAlloc, Requirements> availableByNode ) {
        final Iterator<NodeDataTuple> iterator = nodeDataTuples.iterator();
        while (iterator.hasNext()) {
            if ( !availableByNode.get( iterator.next().getNode() ).higherOrEquals( task.getPod().getRequest() ) ) {
                iterator.remove();
                calc();
                return true;
            }
        }
        return false;
    }

    private void calc() {
        if ( nodeDataTuples.isEmpty() ) value = Double.MIN_VALUE;
        if ( size == 0 ) value = 1;
        else value = nodeDataTuples.get(0).getSizeInBytes() / (double) size;
    }

    @Override
    public int compareTo(@NotNull TaskData o) {
        return Double.compare(getValue(), o.getValue());
    }

    public void addNs( long timeInNs ){
            this.timeInNs += timeInNs;
        }

}