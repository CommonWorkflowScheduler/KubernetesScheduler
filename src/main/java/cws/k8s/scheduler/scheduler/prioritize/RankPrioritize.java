package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.List;

public class RankPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        tasks.sort( ( o1, o2 ) -> {
            if ( o1.getProcess().getRank() == o2.getProcess().getRank() ) {
                return o1.getId() - o2.getId();
            }
            //Prefer larger ranks
            return o2.getProcess().getRank() - o1.getProcess().getRank();
        } );
    }

}
