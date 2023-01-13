package fonda.scheduler.scheduler.la2;

import java.util.Comparator;

public class MinCopyingComparator extends TaskStatComparator {

    public MinCopyingComparator( Comparator<TaskStat.NodeAndStatWrapper> comparator ) {
        super( comparator );
    }

    @Override
    public int compare( TaskStat o1, TaskStat o2 ) {
        if ( o1.getCompleteOnNodes() < o2.getCompleteOnNodes() ) {
            return -1;
        } else if ( o1.getCompleteOnNodes() > o2.getCompleteOnNodes() ) {
            return 1;
        } else if ( o1.getCopyingToNodes() < o2.getCopyingToNodes() ) {
            return -1;
        } else if ( o1.getCopyingToNodes() > o2.getCopyingToNodes() ) {
            return 1;
        } else {
            return getComparator().compare( o1.getBestStats(), o2.getBestStats() );
        }
    }

}
