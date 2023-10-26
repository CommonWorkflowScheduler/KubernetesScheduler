package cws.k8s.scheduler.model;

public enum State {

    RECEIVED_CONFIG(0), UNSCHEDULED(1), SCHEDULED(2), PREPARED(3), INIT_WITH_ERRORS(3), PROCESSING_FINISHED(4),
    FINISHED(5), FINISHED_WITH_ERROR(5), ERROR(1000), DELETED(Integer.MAX_VALUE);

    public final int level;

    State(int level) {
        this.level = level;
    }

}
