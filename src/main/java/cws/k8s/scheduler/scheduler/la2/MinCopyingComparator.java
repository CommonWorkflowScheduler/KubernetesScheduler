package cws.k8s.scheduler.scheduler.la2;

import java.util.Comparator;

/**
 * This comparator first prioritizes tasks that are on fewer nodes yet.
 * If two tasks are on the same number of nodes, the task that is currently copying to fewer nodes is preferred.
 * If two tasks are on the same number of nodes and are copying to the same number of nodes, the comparator
 * that is passed to the constructor is used.
 */
public class MinCopyingComparator extends TaskStatComparator {

    public MinCopyingComparator( Comparator<TaskStat.NodeAndStatWrapper> comparator ) {
        super( comparator );
    }

    @Override
    public int compare( TaskStat o1, TaskStat o2 ) {
        if ( o1.getCompleteOnNodes() != o2.getCompleteOnNodes() ) {
            return Long.compare( o1.getCompleteOnNodes(), o2.getCompleteOnNodes() );
        }
        if ( o1.getCopyingToNodes() != o2.getCopyingToNodes() ) {
            return Long.compare( o1.getCopyingToNodes(), o2.getCopyingToNodes() );
        }
        return getComparator().compare( o1.getBestStats(), o2.getBestStats() );
    }

}
