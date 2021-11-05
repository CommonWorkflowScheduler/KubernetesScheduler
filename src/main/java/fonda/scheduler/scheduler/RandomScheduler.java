package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.model.Task;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class RandomScheduler extends SchedulerWithDaemonSet {

    private final Map<String, String> workdirNode = new HashMap<>();

    public RandomScheduler(String name, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(name, client, namespace, config);
    }

    @Override
    public int schedule( final List<Task> unscheduledTasks ) {
        log.info("Schedule " + this.getName());
        List<NodeWithAlloc> items = getNodeList();
        int unscheduled = 0;
        for ( final Task task : unscheduledTasks) {
            if(isClose()) return -1;
            final Pod pod = task.getPod();
            Optional<NodeWithAlloc> node = items.stream().filter(x -> x.canSchedule(pod) && this.getDaemonOnNode(x) != null).findFirst();
            if( node.isPresent() ){
                log.info("Task needs: " + task.getConfig().getInputs().toString());
                assignPodToNode( task.getPod(), node.get() );
                super.taskWasScheduled( task );
            } else {
                log.info( "No node with enough resources for {}", pod.getMetadata().getName() );
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
