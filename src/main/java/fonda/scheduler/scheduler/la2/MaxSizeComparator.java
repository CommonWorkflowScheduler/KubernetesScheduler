package fonda.scheduler.scheduler.la2;

import fonda.scheduler.util.TaskNodeStats;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Comparator;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MaxSizeComparator implements Comparator<TaskStat.NodeAndStatWrapper> {

    public static final MaxSizeComparator INSTANCE = new MaxSizeComparator();

    @Override
    public int compare( TaskStat.NodeAndStatWrapper o1, TaskStat.NodeAndStatWrapper o2 ) {
        final TaskNodeStats o1Stats = o1.getTaskNodeStats();
        final TaskNodeStats o2Stats = o2.getTaskNodeStats();
        //same task does not necessarily have the same size
        if ( o1.getTask() == o2.getTask() || o1Stats.getTaskSize() == o2Stats.getTaskSize() ) {
            //Prefer the one with fewer remaining data
            return Long.compare( o1Stats.getSizeRemaining(), o2Stats.getSizeRemaining() );
        } else {
            //Prefer the one with larger task size
            return Long.compare( o2Stats.getTaskSize(), o1Stats.getTaskSize() );
        }
    }

}
