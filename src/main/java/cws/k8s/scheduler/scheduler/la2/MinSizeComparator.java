package cws.k8s.scheduler.scheduler.la2;

import cws.k8s.scheduler.util.TaskNodeStats;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Comparator;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MinSizeComparator implements Comparator<TaskStat.NodeAndStatWrapper> {

    public static final MinSizeComparator INSTANCE = new MinSizeComparator();

    @Override
    public int compare( TaskStat.NodeAndStatWrapper o1, TaskStat.NodeAndStatWrapper o2 ) {
        final TaskNodeStats o1Stats = o1.getTaskNodeStats();
        final TaskNodeStats o2Stats = o2.getTaskNodeStats();
        if ( o1Stats.getSizeRemaining() == o2Stats.getSizeRemaining() ) {
            //Prefer the one that is copying fewer data --> expected to finish faster
            return Long.compare( o1Stats.getSizeCurrentlyCopying(), o2Stats.getSizeCurrentlyCopying() );
        } else {
            return Long.compare( o1Stats.getSizeRemaining(), o2Stats.getSizeRemaining() );
        }
    }

}
