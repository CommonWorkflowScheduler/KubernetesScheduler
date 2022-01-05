package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.NodeTaskAlignment;
import fonda.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.util.inputs.Input;
import fonda.scheduler.util.FilePath;
import fonda.scheduler.util.inputs.PathFileLocationTriple;
import fonda.scheduler.util.inputs.SymlinkInput;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class RandomScheduler extends SchedulerWithDaemonSet {

    public RandomScheduler(String name, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(name, client, namespace, config);
    }

    @Override
    public ScheduleObject getTaskNodeAlignment( final List<Task> unscheduledTasks ){
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
                                    //&& !x.getName().equals(this.getWorkflowEngineNode())
                    )
                    .collect(Collectors.toList());
            System.out.println("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest());
            Optional<NodeWithAlloc> node = matchingNodes.isEmpty()
                    ? Optional.empty()
                    : Optional.of(matchingNodes.get(new Random().nextInt(matchingNodes.size())));
            if( node.isPresent() ){
                //log.info("Task needs: " + task.getConfig().getInputs().toString());
                final List<Input> inputsOfTask = getInputsOfTask(task);
                final FileAlignment fileAlignment = scheduleFiles(task, inputsOfTask, node.get());
                alignment.add( new NodeTaskFilesAlignment(node.get(),task, fileAlignment ) );
                availableByNode.get(node.get().getName()).subFromThis(pod.getRequest());
                System.out.println("--> " + node.get().getName());
            } else {
                log.info( "No node with enough resources for {}", pod.getName() );
            }
        }
        System.out.flush();
        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( true );
        return scheduleObject;
    }

    FileAlignment scheduleFiles(Task task, List<Input> inputsOfTask, NodeWithAlloc node) {
        final HashMap<String, List<FilePath>> map = new HashMap<>();
        final List<SymlinkInput> symlinkInputs = new LinkedList<>();
        for ( Input entry : inputsOfTask ) {
            if( entry instanceof PathFileLocationTriple ){
                final PathFileLocationTriple pathFileLocationTriple = (PathFileLocationTriple) entry;
                final LocationWrapper locationWrapper = pathFileLocationTriple.locations.get(
                        new Random().nextInt( pathFileLocationTriple.locations.size() )
                );
                final String nodeIdentifier = locationWrapper.getLocation().getIdentifier();
                if ( !map.containsKey( nodeIdentifier )){
                    map.put( nodeIdentifier, new LinkedList<>() );
                }
                final List<FilePath> pathsOfNode = map.get( nodeIdentifier );
                pathsOfNode.add( new FilePath( pathFileLocationTriple.path.toString(), pathFileLocationTriple.file ) );
            } else if ( entry instanceof SymlinkInput ){
                symlinkInputs.add( (SymlinkInput) entry );
            }
        }
        return new FileAlignment( map, symlinkInputs );
    }

}
