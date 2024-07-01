package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;

public interface CalculateScore {

    /**
     * Score must be higher than 0
     * @return
     */
    long getScore( Task task, NodeLocation location, long inputSize );

}
