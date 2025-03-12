package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.List;

/**
 * This aims to generate samples first for tasks where nothing has yet finished.
 * If not enough samples are available, it will prioritize tasks with the highest
 * rank and largest input size to generate samples faster.
 * If enough samples are available, it will prioritize tasks with the highest rank and largest input.
 */
public class GetSamplesMaxPrioritize implements Prioritize {

    private static final int MAX_FINISHED = 5;

    @Override
    public void sortTasks( List<Task> tasks ) {
        final RankMaxComparator rankMaxComparator = new RankMaxComparator();
        tasks.sort( ( o1, o2 ) -> {
            final int o1Finished = o1.getProcess().getSuccessfullyFinished();
            final int o2Finished = o2.getProcess().getSuccessfullyFinished();
            if ( (o1Finished != o2Finished) && (o1Finished < MAX_FINISHED || o2Finished < MAX_FINISHED) ) {
                return Integer.compare( o1Finished, o2Finished );
            } else {
                return rankMaxComparator.compare( o1, o2 );
            }
        } );
    }

}
