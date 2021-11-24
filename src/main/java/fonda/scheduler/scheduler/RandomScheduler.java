package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.scheduler.util.NodeTaskAlignment;
import fonda.scheduler.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.scheduler.util.PathFileLocationTriple;
import fonda.scheduler.util.FilePath;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class RandomScheduler extends SchedulerWithDaemonSet {

    public RandomScheduler(String name, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(name, client, namespace, config);
    }

    @Override
    public List<NodeTaskAlignment> getTaskNodeAlignment( final List<Task> unscheduledTasks ){
        List<NodeWithAlloc> items = getNodeList();
        List<NodeTaskAlignment> alignment = new LinkedList<>();
        for ( final Task task : unscheduledTasks) {
            final Pod pod = task.getPod();
            final List<NodeWithAlloc> matchingNodes = items.stream().filter(x -> x.canSchedule(pod) && this.getDaemonOnNode(x) != null).collect(Collectors.toList());
            Optional<NodeWithAlloc> node = matchingNodes.isEmpty()
                    ? Optional.empty()
                    : Optional.of(matchingNodes.get(new Random().nextInt(matchingNodes.size())));
            if( node.isPresent() ){
                log.info("Task needs: " + task.getConfig().getInputs().toString());
                final List<PathFileLocationTriple> inputsOfTask = getInputsOfTask(task);
                final Map<String, List<FilePath>> stringListMap = scheduleFiles(task, inputsOfTask, node.get());
                alignment.add( new NodeTaskFilesAlignment(node.get(),task,stringListMap));
            } else {
                log.info( "No node with enough resources for {}", pod.getMetadata().getName() );
            }
        }
        return alignment;
    }

    Map<String, List<FilePath>> scheduleFiles(Task task, List<PathFileLocationTriple> inputsOfTask, NodeWithAlloc node) {
        final HashMap<String, List<FilePath>> map = new HashMap<>();
        for ( PathFileLocationTriple entry : inputsOfTask ) {
            final LocationWrapper locationWrapper = entry.locations.get(new Random().nextInt(entry.locations.size()));
            final String nodeIdentifier = ((NodeLocation) locationWrapper.getLocation()).getIdentifier();
            if ( !map.containsKey( nodeIdentifier )){
                map.put( nodeIdentifier, new LinkedList<>() );
            }
            final List<FilePath> pathsOfNode = map.get( nodeIdentifier );
            pathsOfNode.add( new FilePath( entry.path.toString(), entry.file ) );
        }
        return map;
    }

}
