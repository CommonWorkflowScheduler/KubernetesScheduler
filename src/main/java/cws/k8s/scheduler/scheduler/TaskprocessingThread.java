package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.Task;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class TaskprocessingThread extends Thread {

    private final List<Task> unprocessedTasks;
    private final Function<List<Task>, Integer> function;

    private boolean otherResourceChange = false;

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
                    do {
                        if ( !otherResourceChange && unscheduled == unprocessedTasks.size()) {
                            unprocessedTasks.wait( 10000 );
                        }
                        if( Thread.interrupted() ) {
                            return;
                        }
                        otherResourceChange = false;
                    } while ( unprocessedTasks.isEmpty() );
                    tasks = new LinkedList<>(unprocessedTasks);
                }
                unscheduled = function.apply( tasks );
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            } catch (Exception e){
                unscheduled = -1;
                log.info("Error while processing",e);
            }
        }
    }
}
