package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.scheduler.la2.TaskStat;
import cws.k8s.scheduler.util.TaskNodeStats;
import lombok.RequiredArgsConstructor;

import java.util.Comparator;

@RequiredArgsConstructor
public class MostOutLabelsComparator implements Comparator<TaskStat.NodeAndStatWrapper> {

    private final GroupCluster groupCluster;

    @Override
    public int compare( TaskStat.NodeAndStatWrapper o1, TaskStat.NodeAndStatWrapper o2 ) {
        final TaskNodeStats o1Stats = o1.getTaskNodeStats();
        final TaskNodeStats o2Stats = o2.getTaskNodeStats();
        //same task does not necessarily have the same size
        if ( o1.getTask() == o2.getTask() || o1Stats.getTaskSize() == o2Stats.getTaskSize() ) {
            double ratingO1 = groupCluster.getScoreForTaskOnNode( o1.getTask(), o1.getNode().getNodeLocation() );
            double ratingO2 = groupCluster.getScoreForTaskOnNode( o2.getTask(), o2.getNode().getNodeLocation() );
            // Use the node with a higher rating
            if ( ratingO1 != ratingO2 ) {
                return Double.compare( ratingO2, ratingO1 );
            }
            //Prefer the one with fewer remaining data
            return Long.compare( o1Stats.getSizeRemaining(), o2Stats.getSizeRemaining() );
        } else {
            //Prefer the one with larger task size
            return Long.compare( o2Stats.getTaskSize(), o1Stats.getTaskSize() );
        }
    }

}