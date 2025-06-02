package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.Task;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class TaskprocessingThread extends Thread {

    private final List<Task> unprocessedTasks;
    private final Function<List<Task>, Integer> function;

    private boolean otherResourceChange = false;

    public TaskprocessingThread( String name, List<Task> unprocessedTasks, Function<List<Task>, Integer> function) {
        super(name );
        this.unprocessedTasks = unprocessedTasks;
        this.function = function;
    }

    public void otherResourceChange() {
        otherResourceChange = true;
    }

    @Override
    public void run() {
        int unscheduled = 0;
        while(!Thread.interrupted()){
            try{
                LinkedList<Task> tasks;
                synchronized (unprocessedTasks) {
                    if ( !otherResourceChange && unscheduled == unprocessedTasks.size()) {
                        unprocessedTasks.wait( 1000 );
                    }
                    if( Thread.interrupted() ) {
                        return;
                    }
                    otherResourceChange = false;
                    tasks = new LinkedList<>(unprocessedTasks);
                }
                unscheduled = function.apply( tasks );
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            } catch (Exception e){
                unscheduled = -1;
                log.error("Error while processing",e);
            }
        }
    }
}
