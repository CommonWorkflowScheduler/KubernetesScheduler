package cws.k8s.scheduler.scheduler.la2;

import java.util.Comparator;

public class LeastFinishedFirstMaxRankAndMinCopyingComparator extends RankAndMinCopyingComparator {

    public LeastFinishedFirstMaxRankAndMinCopyingComparator( Comparator<TaskStat.NodeAndStatWrapper> comparator ) {
        super( comparator );
    }

    @Override
    public int compare( TaskStat o1, TaskStat o2 ) {
        final int o1Finished = o1.getTask().getProcess().getSuccessfullyFinished();
        final int o2Finished = o2.getTask().getProcess().getSuccessfullyFinished();
        if ( o1Finished == o2Finished ) {
            return super.compare( o1, o2 );
        } else {
            return Integer.compare( o1Finished, o2Finished );
        }
    }

}
