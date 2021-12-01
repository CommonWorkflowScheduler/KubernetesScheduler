package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.scheduler.util.NodeTaskAlignment;
import fonda.scheduler.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.scheduler.util.PathFileLocationTriple;
import fonda.scheduler.util.FilePath;
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

        Map<String,PodRequirements> availableByNode = new HashMap<>();

        List<String> logInfo = new LinkedList();
        logInfo.add("------------------------------------");
        for (NodeWithAlloc item : items) {
            final PodRequirements availableResources = item.getAvailableResources();
            availableByNode.put(item.getName(), availableResources);
            logInfo.add("Node: " + item.getName() + " " + availableResources);
        }
        logInfo.add("------------------------------------");
        System.out.println(String.join("\n", logInfo));
        
        
        for ( final Task task : unscheduledTasks) {
            final PodWithAge pod = task.getPod();
            final List<NodeWithAlloc> matchingNodes = items
                    .stream()
                    .filter(
                            x -> availableByNode.get(x.getName()).higherOrEquals(pod.getRequest())
                                    && this.getDaemonOnNode(x) != null
                                    && !x.getName().equals(this.getWorkflowEngineNode()))
                    .collect(Collectors.toList());
            System.out.println("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest());
            Optional<NodeWithAlloc> node = matchingNodes.isEmpty()
                    ? Optional.empty()
                    : Optional.of(matchingNodes.get(new Random().nextInt(matchingNodes.size())));
            if( node.isPresent() ){
                //log.info("Task needs: " + task.getConfig().getInputs().toString());
                final List<PathFileLocationTriple> inputsOfTask = getInputsOfTask(task);
                final Map<String, List<FilePath>> stringListMap = scheduleFiles(task, inputsOfTask, node.get());
                alignment.add( new NodeTaskFilesAlignment(node.get(),task,stringListMap) );
                availableByNode.get(node.get().getName()).subFromThis(pod.getRequest());
                System.out.println("--> " + node.get().getName());
            } else {
                log.info( "No node with enough resources for {}", pod.getName() );
            }
        }
        System.out.flush();
        return alignment;
    }

    Map<String, List<FilePath>> scheduleFiles(Task task, List<PathFileLocationTriple> inputsOfTask, NodeWithAlloc node) {
        final HashMap<String, List<FilePath>> map = new HashMap<>();
        for ( PathFileLocationTriple entry : inputsOfTask ) {
            final LocationWrapper locationWrapper = entry.locations.get(new Random().nextInt(entry.locations.size()));
            final String nodeIdentifier = locationWrapper.getLocation().getIdentifier();
            if ( !map.containsKey( nodeIdentifier )){
                map.put( nodeIdentifier, new LinkedList<>() );
            }
            final List<FilePath> pathsOfNode = map.get( nodeIdentifier );
            pathsOfNode.add( new FilePath( entry.path.toString(), entry.file ) );
        }
        return map;
    }

}
