package fonda.scheduler.rest;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.scheduler.RandomScheduler;
import fonda.scheduler.scheduler.Scheduler;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.model.TaskConfig;
import fonda.scheduler.scheduler.SchedulerWithDaemonSet;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@Slf4j
public class SchedulerRestController {

    private final KubernetesClient client;

    /**
     * Holds the scheduler for one execution
     * Execution: String in lowercase
     * Scheduler: An instance of a scheduler with the requested type
     */
    private static final Map< Pair<String,String>, Scheduler > schedulerHolder = new HashMap<>();

    public SchedulerRestController( @Autowired KubernetesClient client ){
        this.client = client;
    }

    public static void addScheduler(Pair<String,String> key, Scheduler scheduler ){
        schedulerHolder.put( key, scheduler );
    }

    /**
     * Register a sheduler for a workflow execution
     * @param namespace namespace where the workflow runs
     * @param execution unique name of the execution
     * @param strategy which scheduling strategy should be used
     * @param config Additional parameters for the scheduler
     * @return
     */
    @PostMapping("/scheduler/registerScheduler/{namespace}/{execution}/{strategy}")
    ResponseEntity registerScheduler(@PathVariable String namespace, @PathVariable String execution, @PathVariable String strategy, @RequestBody(required = false) SchedulerConfig config ) {

        log.trace("Register execution: {} strategy: {} config: {}", execution, strategy, config);

        Scheduler scheduler = null;

        if( schedulerHolder.containsKey( execution.toLowerCase() ) ) {
            return new ResponseEntity( "There is already a scheduler for " + execution, HttpStatus.BAD_REQUEST );
        }

        switch ( strategy.toLowerCase() ){
            case "fifo" :
            case "random" :
            case "fifo-random" : scheduler = new RandomScheduler(execution, client, namespace, config ); break;
            default: return new ResponseEntity( "No scheduler for strategy: " + strategy, HttpStatus.NOT_FOUND );
        }

        final Pair<String, String> key = new Pair(namespace.toLowerCase(), execution.toLowerCase());
        schedulerHolder.put( key, scheduler );

        return new ResponseEntity( HttpStatus.OK );

    }

    /**
     * Register a task for the execution
     * @param namespace namespace where the workflow runs
     * @param execution unique name of the execution
     * @param config The config contains the task name, input files, and optional task parameter the scheduler has to determine
     * @return Parameters the scheduler suggests for the task
     */
    @PostMapping("/scheduler/registerTask/{namespace}/{execution}")
    ResponseEntity registerTask(@PathVariable String namespace, @PathVariable String execution, @RequestBody(required = true) TaskConfig config ) {

        log.trace( execution + " " + config.getTask() + " got: " + config );

        final Pair<String, String> key = new Pair(namespace.toLowerCase(), execution.toLowerCase());
        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null ){
            return new ResponseEntity( "No scheduler for: " + execution , HttpStatus.NOT_FOUND );
        }

        scheduler.addTask( config );
        Map<String, Object> schedulerParams = scheduler.getSchedulerParams(config.getTask(), config.getName());

        return new ResponseEntity( schedulerParams, HttpStatus.OK );

    }

    /**
     * Check Task state
     * @param namespace namespace where the workflow runs
     * @param execution unique name of the execution
     * @param taskid unique name of task (nf-XYZ...)
     * @return boolean
     */
    @GetMapping("/scheduler/taskstate/{namespace}/{execution}/{taskid}")
    ResponseEntity getTaskState(@PathVariable String namespace, @PathVariable String execution, @PathVariable String taskid ) {

        final Pair<String, String> key = new Pair(namespace.toLowerCase(), execution.toLowerCase());
        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null ){
            return new ResponseEntity( "No scheduler for: " + execution , HttpStatus.NOT_FOUND );
        }

        return new ResponseEntity( scheduler.getTaskState( taskid ), HttpStatus.OK );

    }

    /**
     * Call this after the execution has finished
     * @param namespace namespace where the workflow runs
     * @param execution unique name of the execution
     * @return
     */
    @DeleteMapping ("/scheduler/{namespace}/{execution}")
    ResponseEntity delete(@PathVariable String namespace,  @PathVariable String execution) {

        log.info("Delete name: " + execution);
        final Pair<String, String> key = new Pair(namespace.toLowerCase(), execution.toLowerCase());

        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null ){
            return new ResponseEntity( "No scheduler for: " + execution, HttpStatus.NOT_FOUND );
        }
        schedulerHolder.remove( key );
        scheduler.close();
        return new ResponseEntity( HttpStatus.OK );
    }

    @GetMapping("/daemon/{namespace}/{execution}/{node}")
    ResponseEntity getDaemonName(@PathVariable String namespace, @PathVariable String execution, @PathVariable String node ) {

        log.info( "Got request: {}{}{}", namespace, execution, node );

        final Pair<String, String> key = new Pair(namespace.toLowerCase(), execution.toLowerCase());
        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null || !(scheduler instanceof SchedulerWithDaemonSet) ){
            return new ResponseEntity( "No scheduler for: " + execution , HttpStatus.NOT_FOUND );
        }

        String daemon = ((SchedulerWithDaemonSet) scheduler).getDaemonOnNode( node );

        if ( daemon == null ){
            return new ResponseEntity( "No daemon for node found: " + node , HttpStatus.NOT_FOUND );
        }

        return new ResponseEntity( daemon, HttpStatus.OK );

    }

    @GetMapping ("/health")
    ResponseEntity checkHealth() {
        return new ResponseEntity( HttpStatus.OK );
    }

}
