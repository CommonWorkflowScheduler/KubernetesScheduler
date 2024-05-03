package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.dag.Process;

public class TestProcess extends Process {

    final private int finishedTasks;
    final private int rank;

    /**
     * Only public for tests
     *
     * @param label
     * @param uid
     */
    public TestProcess( String label, int uid, int finishedTasks, int rank ) {
        super( label, uid );
        this.finishedTasks = finishedTasks;
        this.rank = rank;
    }

    @Override
    public int getRank() {
        return rank;
    }

    @Override
    public int getSuccessfullyFinished() {
        return finishedTasks;
    }

}
