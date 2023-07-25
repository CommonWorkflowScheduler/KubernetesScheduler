package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.Task;

public class FileSizeScore implements CalculateScore {

    @Override
    public long getScore( Task task, long size ) {
        //add one to prefer two tasks which sum up to the same score otherwise
        return size + 1;
    }
}
