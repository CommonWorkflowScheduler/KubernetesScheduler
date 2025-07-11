package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;

public class FileSizeScore implements CalculateScore {

    @Override
    public long getScore( Task task, NodeWithAlloc location, long size ) {
        //add one to prefer two tasks which sum up to the same score otherwise
        long score = (long) Math.pow( size, 0.83 );
        return score + 1;
    }
}
