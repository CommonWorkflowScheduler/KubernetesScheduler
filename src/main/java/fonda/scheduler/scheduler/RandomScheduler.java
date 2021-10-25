package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.NodeWithAlloc;
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
    public void schedule( final List<Pod> unscheduledPods ) {
        List<NodeWithAlloc> items = getNodeList();
        for ( final Pod pod : unscheduledPods) {
            if(isClose()) return;
            Optional<NodeWithAlloc> node = items.stream().filter(x -> x.canSchedule(pod)).findFirst();
            if( node.isPresent() ){
                assignPodToNode( pod, node.get() );
                super.scheduledPod( pod );
            } else {
                log.info( "No node with enough resources for {}", pod.getMetadata().getName() );
            }
        }
    }

    @Override
    void podEventReceived(Watcher.Action action, Pod pod){}
}
