package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.Task;

public class FileSizeScore implements CalculateScore {

    @Override
    public long getScore( Task task, long size ) {
        return size;
    }
}
