package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.scheduler.la2.RankAndMinCopyingComparator;
import cws.k8s.scheduler.scheduler.la2.TaskStat;

import java.util.Comparator;

public class RankAndMinCopyingComparatorCopyTasks extends RankAndMinCopyingComparator {

    public RankAndMinCopyingComparatorCopyTasks( Comparator<TaskStat.NodeAndStatWrapper> comparator ) {
        super( comparator );
    }

    @Override
    public int compare( TaskStat o1, TaskStat o2 ) {
        if ( o1.getTask() instanceof CopyTask ^ o2.getTask() instanceof CopyTask ) {
            if ( o1.getTask() instanceof CopyTask ) {
                return 1;
            } else {
                return -1;
            }
        } else {
            return super.compare( o1, o2 );
        }
    }

}
