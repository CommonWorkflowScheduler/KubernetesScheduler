package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.Task;

public interface CalculateScore {

    /**
     * Score must be higher than 0
     * @return
     */
    long getScore( Task task, long inputSize );

}
