package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.List;

/**
 * This prioritizer prioritizes tasks that have the least finished instances.
 * To break ties, it follows the rank-max strategy.
 * The approach is based on Witt et al. Feedback-Based Resource Allocation for Batch Scheduling of Scientific Workflows (2019).
 */
public class LeastFinishedFirstPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        final RankMaxComparator rankMaxComparator = new RankMaxComparator();
        tasks.sort( ( o1, o2 ) -> {
            final int o1Finished = o1.getProcess().getSuccessfullyFinished();
            final int o2Finished = o2.getProcess().getSuccessfullyFinished();
            if ( o1Finished == o2Finished ) {
                return rankMaxComparator.compare( o1, o2 );
            } else {
                return Integer.compare( o1Finished, o2Finished );
            }
        } );
    }

}
