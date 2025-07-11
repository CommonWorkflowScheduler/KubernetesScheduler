package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;

public class LFFFileSizeRankScore extends FileSizeRankScore {

    // Allows 100 task instances, otherwise it ignores it
    protected static final long LFFNumber = 1_000_000_000_000L;

    @Override
    public long getScore( Task task, NodeWithAlloc location, long size ) {

        // if more than 100 tasks are finished, ignore this
        int remaining = Math.max(0, 100 - task.getProcess().getSuccessfullyFinished() );

        return super.getScore( task, location, size ) + remaining * LFFNumber;
    }

}
