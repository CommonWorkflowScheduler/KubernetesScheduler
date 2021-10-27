package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.State;
import fonda.scheduler.model.Task;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class RandomScheduler extends Scheduler {


    private final Map<String, String> daemonByNode = new HashMap<>();
    private final Map<String, String> workdirNode = new HashMap<>();

    public RandomScheduler(String name, KubernetesClient client, String namespace) {
        super(name, client, namespace);
    }

    @Override
    public int schedule( final List<Task> unscheduledTasks ) {
        log.info("Schedule " + this.getName());
        List<NodeWithAlloc> items = getNodeList();
        int unscheduled = 0;
        for ( final Task task : unscheduledTasks) {
            if(isClose()) return -1;
            Optional<NodeWithAlloc> node = items.stream().filter(x -> x.canSchedule(task.getPod())).findFirst();
            if( node.isPresent() ){
                log.info("Task needs: " + task.getConfig().getInputs().toString());
                assignPodToNode( task.getPod(), node.get() );
                super.taskWasScheduled( task );
            } else {
                log.info( "No node with enough resources for {}", task.getPod().getMetadata().getName() );
                unscheduled++;
            }
        }
        return unscheduled;
    }

    @Override
    int terminateTasks(List<Task> finishedTasks) {
        for (Task finishedTask : finishedTasks) {
            super.taskWasFinished( finishedTask );
        }
        return 0;
    }

    @Override
    void podEventReceived(Watcher.Action action, Pod pod){}
}
