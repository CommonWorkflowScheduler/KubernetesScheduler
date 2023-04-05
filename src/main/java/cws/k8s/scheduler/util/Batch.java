package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
public class Batch {

    @Getter
    boolean closed = false;

    public final int id;
    private List<Task> ready = new LinkedList<>();
    private Set<Task> unready = new HashSet<>();
    private int tasksInBatch = -1;

    private final long createTime = System.currentTimeMillis();

    private long closeTime;

    public void close( int tasksInBatch ){
        this.closed = true;
        this.tasksInBatch = tasksInBatch;
        closeTime = System.currentTimeMillis();
    }

    public void registerTask( Task task ){
        assert ready != null;
        assert unready != null;

        synchronized ( unready ){
            if ( closed && ready.size() + unready.size() >= tasksInBatch ) {
                throw new IllegalStateException("Batch was closed!");
            }
            unready.add( task );
        }
        task.setBatch( this );
        final TraceRecord traceRecord = task.getTraceRecord();
        traceRecord.setSchedulerBatchId( id );
        traceRecord.setSchedulerDeltaBatchStartSubmitted((int) (System.currentTimeMillis() - createTime));

    }

    public void informSchedulable( Task task ){
        synchronized ( unready ){
            final boolean remove = unready.remove(task);
            if ( remove ) {
                ready.add( task );
            }
        }
        task.getTraceRecord().setSchedulerDeltaBatchStartReceived((int) (System.currentTimeMillis() - createTime));
    }

    public boolean canSchedule(){
        return closed && unready.isEmpty();
    }

    public List<Task> getTasksToScheduleAndDestroy(){
        if ( !closed ) {
            throw new IllegalStateException("Batch was not yet closed!");
        }
        final List<Task> readyList = this.ready;
        long start = System.currentTimeMillis();
        int deltaCloseEnd = (int) (start - closeTime);
        readyList.parallelStream().forEach( task -> {
            final TraceRecord traceRecord = task.getTraceRecord();
            traceRecord.setSchedulerDeltaSubmittedBatchEnd((int) (start - createTime - traceRecord.getSchedulerDeltaBatchStartSubmitted()));
            traceRecord.setSchedulerDeltaBatchClosedBatchEnd( deltaCloseEnd );
        });
        this.ready = null;
        this.unready = null;
        return readyList;
    }

}
