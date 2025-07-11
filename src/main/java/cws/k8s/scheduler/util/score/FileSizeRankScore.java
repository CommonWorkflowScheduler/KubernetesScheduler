package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;

public class FileSizeRankScore extends FileSizeScore {

    protected static final long LARGENUMBER = 10_000_000_000L;

    @Override
    public long getScore( Task task, NodeWithAlloc location, long size ) {
        //Add one to avoid becoming zero
        final int rank = task.getProcess().getRank() + 1;
        final long rankFactor = LARGENUMBER * rank;
        return super.getScore( task, location, size ) + rankFactor ;
    }

}
