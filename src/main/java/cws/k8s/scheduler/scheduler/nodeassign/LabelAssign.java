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

            try {
                taskName = task.getConfig().getName();
                taskLabel = taskName.split("~")[1]; 
                // ~ is used for a special case in which subtasks from one process in nextflow are generated
                // the labels in the nextflow config have to be named like this: ~label~

                log.info("Label for task: " + task.getConfig().getName() + " : " + taskLabel);
            } catch ( Exception e ){
                log.warn( "Cannot find a label for task: " + task.getConfig().getName(), e );
                continue;
            }            

            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest() );

            if(nodelabel.containsKey(taskLabel)){
                String nodeName = nodelabel.get(taskLabel);

                for ( Map.Entry<NodeWithAlloc, Requirements> e : entries ) {
                    final NodeWithAlloc node = e.getKey();

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
                log.info( "Task Label: " + taskLabel + " doesn't exist in config file.");
            }
        }
        return alignment;
    }
}

