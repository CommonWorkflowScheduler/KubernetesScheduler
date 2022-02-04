package fonda.scheduler.rest;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.rest.exceptions.NotARealFileException;
import fonda.scheduler.rest.response.getfile.FileResponse;
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
    @PutMapping("/scheduler/registerScheduler/{namespace}/{execution}/{strategy}")
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

        final Pair<String, String> key = getKey( namespace, execution );
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
    @PutMapping("/scheduler/registerTask/{namespace}/{execution}")
    ResponseEntity registerTask(@PathVariable String namespace, @PathVariable String execution, @RequestBody(required = true) TaskConfig config ) {

        log.trace( execution + " " + config.getTask() + " got: " + config );

        final Pair<String, String> key = getKey( namespace, execution );
        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null ){
            return new ResponseEntity( "No scheduler for: " + execution , HttpStatus.NOT_FOUND );
        }

        scheduler.addTask( config );
        Map<String, Object> schedulerParams = scheduler.getSchedulerParams(config.getTask(), config.getName());

        return new ResponseEntity( schedulerParams, HttpStatus.OK );

    }

    @PostMapping("/scheduler/startBatch/{namespace}/{execution}")
    ResponseEntity startBatch(@PathVariable String namespace, @PathVariable String execution ) {

        final Pair<String, String> key = getKey( namespace, execution );
        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null ){
            return new ResponseEntity( "No scheduler for: " + execution , HttpStatus.NOT_FOUND );
        }
        scheduler.startBatch();
        return new ResponseEntity( HttpStatus.OK );

    }

    @PostMapping("/scheduler/endBatch/{namespace}/{execution}")
    ResponseEntity endBatch(@PathVariable String namespace, @PathVariable String execution, @RequestBody(required = true) int tasksInBatch ) {

        final Pair<String, String> key = getKey( namespace, execution );
        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null ){
            return new ResponseEntity( "No scheduler for: " + execution , HttpStatus.NOT_FOUND );
        }
        scheduler.endBatch( tasksInBatch );
        return new ResponseEntity( HttpStatus.OK );

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

        final Pair<String, String> key = getKey( namespace, execution );
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
        final Pair<String, String> key = getKey( namespace, execution );

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

        final Pair<String, String> key = getKey( namespace, execution );
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

    @GetMapping("/file/{namespace}/{execution}")
    ResponseEntity getNodeForFile(@PathVariable String namespace, @PathVariable String execution, @RequestParam String path ) {

        log.info( "Get file location request: {} {} {}", namespace, execution, path );

        final Pair<String, String> key = getKey( namespace, execution );
        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null || !(scheduler instanceof SchedulerWithDaemonSet) ){
            return new ResponseEntity( "No scheduler for: " + execution , HttpStatus.NOT_FOUND );
        }

        FileResponse fileResponse = null;
        try {
            fileResponse = ((SchedulerWithDaemonSet) scheduler).nodeOfLastFileVersion( path );
            log.info(fileResponse.toString());
        } catch (NotARealFileException e) {
            return new ResponseEntity( "Requested path is not a real file: " + path , HttpStatus.BAD_REQUEST );
        }

        if ( fileResponse == null ){
            return new ResponseEntity( "No node for file found: " + path , HttpStatus.NOT_FOUND );
        }

        return new ResponseEntity( fileResponse, HttpStatus.OK );

    }

    @PostMapping("/file/location/{method}/{namespace}/{execution}")
    ResponseEntity changeLocationForFile(@PathVariable String method, @PathVariable String namespace, @PathVariable String execution, @RequestBody PathAttributes pa ) {
        return changeLocationForFile(method,namespace,execution,null,pa);
    }

    @PostMapping("/file/location/{method}/{namespace}/{execution}/{node}")
    ResponseEntity changeLocationForFile(@PathVariable String method, @PathVariable String namespace, @PathVariable String execution, @PathVariable String node, @RequestBody PathAttributes pa ) {

        log.info( "Change file location request: {} {} {} {}", method, namespace, execution, pa );

        final Pair<String, String> key = getKey( namespace, execution );
        final Scheduler scheduler = schedulerHolder.get( key );
        if( scheduler == null || !(scheduler instanceof SchedulerWithDaemonSet) ){
            log.info("No scheduler for: " + execution);
            return new ResponseEntity( "No scheduler for: " + execution , HttpStatus.NOT_FOUND );
        }

        if ( !method.equals("add") && !method.equals("overwrite") ) {
            log.info("Method not found: " + method);
            return new ResponseEntity( "Method not found: " + method , HttpStatus.NOT_FOUND );
        }

        boolean overwrite = method.equals("overwrite");

        ((SchedulerWithDaemonSet) scheduler).addFile( pa.getPath(), pa.getSize(), pa.getTimestamp(), overwrite, node );

        return new ResponseEntity( HttpStatus.OK );

    }

    @GetMapping ("/health")
    ResponseEntity checkHealth() {
        return new ResponseEntity( HttpStatus.OK );
    }

    private Pair getKey(String namespace, String execution ){
        return new Pair(namespace.toLowerCase(), execution.toLowerCase());
    }

}
