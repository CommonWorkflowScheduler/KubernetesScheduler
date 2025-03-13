package cws.k8s.scheduler.scheduler.la2;

import java.util.Comparator;

/**
 * This comparator first prioritizes tasks with a higher rank.
 * If two tasks have the same rank, the comparator that is passed to the constructor is used.
 */
public class RankAndMinCopyingComparator extends MinCopyingComparator {

    public RankAndMinCopyingComparator( Comparator<TaskStat.NodeAndStatWrapper> comparator ) {
        super( comparator );
    }

    @Override
    public int compare( TaskStat o1, TaskStat o2 ) {
        final int rankO1 = o1.getTask().getProcess().getRank();
        final int rankO2 = o2.getTask().getProcess().getRank();
        if ( rankO1 == rankO2 ) {
            return super.compare( o1, o2 );
        } else {
            //Prefer larger rank
            return Integer.compare( rankO2, rankO1  );
        }
    }

}
