package fonda.scheduler.scheduler;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;
import java.util.*;

@Slf4j
public class SchedulerThread extends Thread {

    private final List<Pod> unscheduledPods;
    private final Scheduler scheduler;

    public SchedulerThread(List<Pod> unscheduledPods, Scheduler scheduler) {
        this.unscheduledPods = unscheduledPods;
        this.scheduler = scheduler;
    }

    @Override
    public void run() {
        while(!Thread.interrupted()){
            try{
                synchronized (unscheduledPods) {
                    if (unscheduledPods.isEmpty()) {
                        unscheduledPods.wait( 10000 );
                    }
                }
                scheduler.schedule( new LinkedList<>(unscheduledPods) );
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            } catch (Exception e){
                log.info("Error while scheduling",e);
            }
        }
    }
}
