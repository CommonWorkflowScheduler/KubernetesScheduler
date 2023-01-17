package fonda.scheduler.scheduler.la2;

import java.util.Comparator;

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
