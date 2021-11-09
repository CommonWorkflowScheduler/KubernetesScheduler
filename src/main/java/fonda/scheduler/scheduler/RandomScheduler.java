package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.model.Task;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
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
    void assignTaskToNode( Task task, NodeWithAlloc node ) {

        //Create initData
        log.info( "Task: {}", task );

        File file = new File(task.getWorkingDir() + '/' + ".command.init");

        PrintWriter pw = null;
        try {
            pw = new PrintWriter( file );
            pw.println( "echo \"Task init successful\"" );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if ( pw != null ){
                pw.close();
            }
        }

        super.assignTaskToNode(task, node);
    }

    @Override
    int terminateTasks(List<Task> finishedTasks) {
        for (Task finishedTask : finishedTasks) {
            //TODO store new Data
            super.taskWasFinished( finishedTask );
        }
        return 0;
    }

}
