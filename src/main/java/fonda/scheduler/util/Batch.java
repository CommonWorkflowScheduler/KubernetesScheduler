package fonda.scheduler.util;

import fonda.scheduler.model.Task;
import lombok.Getter;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Batch {

    @Getter
    boolean closed = false;

    public final int id;
    private List<Task> ready = new LinkedList<>();
    private Set<Task> unready = new HashSet<>();
    private int tasksInBatch = -1;

    public Batch(int id) {
        this.id = id;
    }

    public void close( int tasksInBatch ){
        this.closed = true;
        this.tasksInBatch = tasksInBatch;
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
    }

    public void informScheduable( Task task ){
        synchronized ( unready ){
            final boolean remove = unready.remove(task);
            if ( remove ) ready.add( task );
        }
    }

    public boolean canSchedule(){
        return closed && unready.isEmpty();
    }

    public List<Task> getTasksToScheduleAndDestroy(){
        if ( !closed ) throw new IllegalStateException("Batch was not yet closed!");
        final List<Task> readyList = this.ready;
        this.ready = null;
        this.unready = null;
        return readyList;
    }

}
