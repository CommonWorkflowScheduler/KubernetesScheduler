package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class RandomScheduler extends SchedulerWithDaemonSet {

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
            final List<NodeWithAlloc> matchingNodes = items.stream().filter(x -> x.canSchedule(pod) && this.getDaemonOnNode(x) != null).collect(Collectors.toList());
            Optional<NodeWithAlloc> node = matchingNodes.isEmpty()
                    ? Optional.empty()
                    : Optional.of(matchingNodes.get(new Random().nextInt(matchingNodes.size())));
            if( node.isPresent() ){
                log.info("Task needs: " + task.getConfig().getInputs().toString());
                if ( !getInputsFromNodes( task, node.get() ) ) continue;
                assignTaskToNode( task, node.get() );
                super.taskWasScheduled( task );
            } else {
                log.info( "No node with enough resources for {}", pod.getMetadata().getName() );
                unscheduled++;
            }
        }
        return unscheduled;
    }

    @Override
    Map<String, List<String>> scheduleFiles(Task task, Map<Path, List<LocationWrapper>> inputsOfTask, NodeWithAlloc node) {
        final HashMap<String, List<String>> map = new HashMap<>();
        for (Map.Entry<Path, List<LocationWrapper>> entry : inputsOfTask.entrySet()) {
            final LocationWrapper locationWrapper = entry.getValue().get(new Random().nextInt(entry.getValue().size()));
            final String nodeIdentifier = ((NodeLocation) locationWrapper.getLocation()).getIdentifier();
            if ( !map.containsKey( nodeIdentifier )){
                map.put( nodeIdentifier, new LinkedList<>() );
            }
            final List<String> pathsOfNode = map.get( nodeIdentifier );
            pathsOfNode.add( entry.getKey().toString() );
        }
        return map;
    }

}
