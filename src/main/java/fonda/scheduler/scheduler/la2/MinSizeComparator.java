package fonda.scheduler.scheduler.la2;

import fonda.scheduler.util.TaskNodeStats;
import lombok.NoArgsConstructor;

import java.util.Comparator;

@NoArgsConstructor
public class MinSizeComparator implements Comparator<TaskStat.NodeAndStatWrapper> {

    public final static MinSizeComparator INSTANCE = new MinSizeComparator();

    @Override
    public int compare( TaskStat.NodeAndStatWrapper o1, TaskStat.NodeAndStatWrapper o2 ) {
        final TaskNodeStats o1Stats = o1.getTaskNodeStats();
        final TaskNodeStats o2Stats = o2.getTaskNodeStats();
        if ( o1Stats.getSizeRemaining() < o2Stats.getSizeRemaining() ) {
            return -1;
        } else if ( o1Stats.getSizeRemaining() > o2Stats.getSizeRemaining() ) {
            return 1;
        } else {
            return Long.compare( o1Stats.getSizeCurrentlyCopying(), o2Stats.getSizeCurrentlyCopying() );
        }
    }

}
