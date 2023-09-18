package cws.k8s.scheduler.scheduler.nodeassign;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class LabelAssign extends NodeAssign {

    public Map<String, String> nodelabel;

    public LabelAssign( 
                        final Map<String, String> nodelabel
                     ){
        this.nodelabel = nodelabel;
    }
    
    @Override
    public List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode ) {
        LinkedList<NodeTaskAlignment> alignment = new LinkedList<>();
        final ArrayList<Map.Entry<NodeWithAlloc, Requirements>> entries = new ArrayList<>( availableByNode.entrySet() );
        for ( final Task task : unscheduledTasks ) {
            
            String taskName = null; 
            String taskLabel = null;        

            // try this else do another assign approach 
            try {
                // taskLabel = task.getConfig().getInputs().getStringInputs().get(0).value;
                taskName = task.getConfig().getName();
                taskLabel = taskName.split("~")[1];

                log.info("Label for task: " + task.getConfig().getName() + " == " + taskLabel);
            } catch ( Exception e ){
                log.warn( "Cannot find a label for task: " + task.getConfig().getName(), e );
                continue;
            }            

            // System.out.println("Task Label: " + taskLabel);

            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest() );

            if(nodelabel.containsKey(taskLabel)){
                String nodeName = nodelabel.get(taskLabel);
                // System.out.println("\n\nTask Node Name: " + nodeName);

                for ( Map.Entry<NodeWithAlloc, Requirements> e : entries ) {
                    final NodeWithAlloc node = e.getKey();

                    // System.out.println("Node Name: " + node.getName());
                    // System.out.println("Equals? " + nodeName + " == " + node.getName() + "    " + (nodeName.equals(node.getName())));
                    if(nodeName.equals(node.getName())){
                        System.out.println("Aligned Pod to node: " + node.getName());
                        alignment.add( new NodeTaskAlignment( node, task ) );
                        availableByNode.get( node ).subFromThis(pod.getRequest());
                        log.info("--> " + node.getName());
                        task.getTraceRecord().foundAlignment();
                        break;
                    } 
                }
            } else 
            {
                log.info( "Task Label: " + taskLabel + " doesn't exist in config file. Please edit your config file.");
            }
        }
        return alignment;
    }
}

