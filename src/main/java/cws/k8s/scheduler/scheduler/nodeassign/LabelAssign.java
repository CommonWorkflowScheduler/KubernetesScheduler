package cws.k8s.scheduler.scheduler.nodeassign;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

@Slf4j
public class LabelAssign extends NodeAssign {

    final SchedulerConfig config;

    public LabelAssign( 
                        final SchedulerConfig config
                     ){
        this.config = config;
    }
    
    @Override
    public List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode ) {
        
        // get the node-label map 
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String,String> nodelabel = objectMapper.convertValue(config.additional.get("tasklabelconfig"),Map.class);

        if ( nodelabel == null ){
            log.error("No tasklabelconfig exist in the nextflow.config file. Define a tasklabelconfig or use another scheduling strategy.");
        }

        LinkedList<NodeTaskAlignment> alignment = new LinkedList<>();
        // final ArrayList<Map.Entry<NodeWithAlloc, Requirements>> entries = new ArrayList<>( availableByNode.entrySet() );
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
            // log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest() );

            if(nodelabel.containsKey(taskLabel)){
                String nodeName = nodelabel.get(taskLabel);

                for ( Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet() ) {
                    final NodeWithAlloc node = e.getKey();

                    if(nodeName.equals(node.getName())){
                        log.info("Aligned Pod to node: " + node.getName());
                        alignment.add( new NodeTaskAlignment( node, task ) );
                        availableByNode.get( node ).subFromThis(pod.getRequest());
                        log.info("--> " + node.getName());
                        task.getTraceRecord().foundAlignment();
                        break;
                    } 
                }
            } else {
                log.warn( "Task Label: " + taskLabel + " does not exist in config file.");
            }
        }
        return alignment;
    }
}

