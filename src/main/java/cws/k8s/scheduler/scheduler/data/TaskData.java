package cws.k8s.scheduler.scheduler.data;

import cws.k8s.scheduler.scheduler.MatchingFilesAndNodes;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
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

    private boolean weightWasSet;

    @Setter
    private NodeLocation outLabelNode = null;

    @Setter
    private double weight = 1.0;

    public TaskData(
            double size,
            Task task,
            List<NodeDataTuple> nodeDataTuples,
            MatchingFilesAndNodes matchingFilesAndNodes,
            double antiStarvingFactor,
            boolean weightWasSet ) {
        this.size = size;
        this.task = task;
        this.nodeDataTuples = nodeDataTuples;
        this.matchingFilesAndNodes = matchingFilesAndNodes;
        this.antiStarvingFactor = antiStarvingFactor;
        this.weightWasSet = weightWasSet;
        calc();
    }


    /**
     * After a location for a label was found, adjust the nodes.
     * @param nodeForLabel
     * @param weight
     */
    public void setNodeAndWeight(NodeLocation nodeForLabel, double weight ) {
        this.outLabelNode = nodeForLabel;
        this.weight = weight;
        final Iterator<NodeDataTuple> iterator = nodeDataTuples.iterator();
        NodeDataTuple onOutLabelNode = null;
        while ( iterator.hasNext() ) {
            final NodeDataTuple next = iterator.next();
            if  (next.getNode().getNodeLocation() != nodeForLabel ) {
                next.setWeight( weight );
            } else {
                onOutLabelNode = next;
                iterator.remove();
            }
        }
        //Insert the one element manuel. Sorting would be too expensive.
        if ( onOutLabelNode != null ) {
            final ListIterator<NodeDataTuple> listIterator = nodeDataTuples.listIterator();
            while ( listIterator.hasNext() ) {
                if ( listIterator.next().getWorth() <= onOutLabelNode.getWorth() ) {
                    listIterator.previous();
                    listIterator.add( onOutLabelNode );
                    break;
                }
            }
        }
        weightWasSet = true;
    }

    /**
     * Recalculate the value. New value is smaller or equal old value
     * Delete all nodes that have not enough unassigned resources for the task.
     * @param availableByNode
     * @return if the value has changed
     */
    public boolean calculate( Map<NodeWithAlloc, Requirements> availableByNode ) {
        final Iterator<NodeDataTuple> iterator = nodeDataTuples.iterator();
        boolean changed = false;
        while (iterator.hasNext()) {
            if ( !availableByNode.get( iterator.next().getNode() ).higherOrEquals( task.getPod().getRequest() ) ) {
                iterator.remove();
                changed = true;
            } else {
                break;
            }
        }
        calc();
        return changed;
    }

    private void calc() {
        if ( nodeDataTuples.isEmpty() ) {
            value = Double.MIN_VALUE;
        } else if ( size == 0 ) {
            value = Double.MAX_VALUE;
        } else {
            value = size;//nodeDataTuples.get(0).getSizeInBytes() / size;
        }
    }

    @Override
    public int compareTo(@NotNull TaskData o) {
        return Double.compare(getValue(), o.getValue());
    }

    public void addNs( long timeInNs ){
            this.timeInNs += timeInNs;
        }

}