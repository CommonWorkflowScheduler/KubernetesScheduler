package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.Task;

public class FileSizeRankScore extends FileSizeScore {

    @Override
    public long getScore( Task task, long size ) {
        //Add one to avoid becoming zero
        final int rank = task.getProcess().getRank() + 1;
        final long rankFactor = 100_000_000_000_000L * rank; // long would allow a rank of 92233
        return super.getScore( task, size ) + rankFactor ;
    }

}
