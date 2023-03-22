package cws.k8s.scheduler.scheduler.la2;

import cws.k8s.scheduler.util.TaskNodeStats;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Comparator;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MaxPartSizeComparator implements Comparator<TaskStat.NodeAndStatWrapper> {

    public static final MaxPartSizeComparator INSTANCE = new MaxPartSizeComparator();

    @Override
    public int compare( TaskStat.NodeAndStatWrapper o1, TaskStat.NodeAndStatWrapper o2 ) {
        final TaskNodeStats o1Stats = o1.getTaskNodeStats();
        final TaskNodeStats o2Stats = o2.getTaskNodeStats();
        final double o1TaskSize = o1Stats.getTaskSize();
        final double o2TaskSize = o2Stats.getTaskSize();

        //Prefer o2 if o1 is missing more percent of data
        if ( o1Stats.getSizeRemaining() / o1TaskSize > o2Stats.getSizeRemaining() / o2TaskSize ) {
            return 1;
        } else if ( o1Stats.getSizeRemaining() / o1TaskSize < o2Stats.getSizeRemaining() / o2TaskSize ) {
            return -1;
        } else {
            //Prefer o1 if o1 is copying more percent of data
            return Double.compare(
                    o2Stats.getSizeCurrentlyCopying() / o2TaskSize,
                    o1Stats.getSizeCurrentlyCopying() / o1TaskSize
            );
        }
    }

}
