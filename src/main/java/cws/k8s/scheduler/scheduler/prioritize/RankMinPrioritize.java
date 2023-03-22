package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.List;

public class RankMinPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        tasks.sort( ( o1, o2 ) -> {
            if ( o1.getProcess().getRank() == o2.getProcess().getRank() ) {
                return Long.signum( o1.getInputSize() - o2.getInputSize());
            }
            //Prefer larger ranks
            return Integer.signum( o2.getProcess().getRank() - o1.getProcess().getRank() );
        } );
    }

}
