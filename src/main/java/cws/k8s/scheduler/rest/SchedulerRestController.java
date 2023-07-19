package cws.k8s.scheduler.rest;

import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.*;
import cws.k8s.scheduler.scheduler.filealignment.GreedyAlignment;
import cws.k8s.scheduler.scheduler.filealignment.costfunctions.CostFunction;
import cws.k8s.scheduler.scheduler.filealignment.costfunctions.MinSizeCost;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.InputEdge;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.scheduler.prioritize.*;
import cws.k8s.scheduler.rest.exceptions.NotARealFileException;
import cws.k8s.scheduler.rest.response.getfile.FileResponse;
import cws.k8s.scheduler.scheduler.la2.ready2run.OptimalReadyToRunToNode;
import cws.k8s.scheduler.scheduler.nodeassign.FairAssign;
import cws.k8s.scheduler.scheduler.nodeassign.NodeAssign;
import cws.k8s.scheduler.scheduler.nodeassign.RandomNodeAssign;
import cws.k8s.scheduler.scheduler.nodeassign.RoundRobinAssign;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@EnableScheduling
public class SchedulerRestController {

    private final KubernetesClient client;
    private final boolean autoClose;
    private final ApplicationContext appContext;
    private long closedLastScheduler = -1;

    /**
     * Holds the scheduler for one execution
     * Execution: String in lowercase
     * Scheduler: An instance of a scheduler with the requested type
     */
    private static final Map<String, Scheduler> schedulerHolder = new HashMap<>();

    public SchedulerRestController(
            @Autowired KubernetesClient client,
            @Value("#{environment.AUTOCLOSE}") String autoClose,
            @Autowired ApplicationContext appContext ){
        this.client = client;
        this.autoClose = Boolean.parseBoolean(autoClose);
        this.appContext = appContext;
    }

    @Scheduled(fixedDelay = 5000)
    public void close() throws InterruptedException {
        if ( autoClose && schedulerHolder.isEmpty() && closedLastScheduler != -1 ) {
            Thread.sleep( System.currentTimeMillis() - closedLastScheduler + 5000 );
            if ( schedulerHolder.isEmpty() ) {
                SpringApplication.exit(appContext, () -> 0);
            }
        }
    }

    private ResponseEntity<String> noSchedulerFor( String execution ){
        log.warn( "No scheduler for execution: {}", execution );
        return new ResponseEntity<>( "There is no scheduler for " + execution, HttpStatus.BAD_REQUEST );
    }

    @Operation(summary = "Register a new execution")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Execution successfully registered",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Scheduling algorithm or cost function not found",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "Scheduling algorithm does not work with current config",
                    content = @Content) })
    /**
     * Register a scheduler for a workflow execution
     * @param execution unique name of the execution
     * @param config Additional parameters for the scheduler
     * @return
     */
    @PostMapping("/v1/scheduler/{execution}")
    ResponseEntity<String> registerScheduler(
            @PathVariable String execution,
            @RequestBody(required = false) SchedulerConfig config
    ) {

        final String namespace = config.namespace;
        final String strategy = config.strategy;
        log.info( "Register execution: {} strategy: {} cf: {} config: {}", execution, strategy, config.costFunction, config );

        Scheduler scheduler;


        if ( schedulerHolder.containsKey( execution ) ) {
            return noSchedulerFor( execution );
        }

        CostFunction costFunction = null;
        if ( config.costFunction != null ) {
            switch (config.costFunction.toLowerCase()) {
                case "minsize": costFunction = new MinSizeCost(0); break;
                default: return new ResponseEntity<>( "No cost function: " + config.costFunction, HttpStatus.NOT_FOUND );
            }
        }

        switch ( strategy.toLowerCase() ){
            case "lav1" :
                if ( !config.locationAware ) {
                    return new ResponseEntity<>( "LA scheduler only works if location aware", HttpStatus.BAD_REQUEST );
                }
                if ( costFunction == null ) {
                    costFunction = new MinSizeCost( 0 );
                }
                scheduler = new LASchedulerV1( execution, client, namespace, config, new GreedyAlignment( 0.5, costFunction ) );
                break;
            case "lav2" :
                if ( !config.locationAware ) {
                    return new ResponseEntity<>( "LA scheduler only works if location aware", HttpStatus.BAD_REQUEST );
                }
                if ( costFunction == null ) {
                    costFunction = new MinSizeCost( 0 );
                }
                scheduler = new LocationAwareSchedulerV2( execution, client, namespace, config, new GreedyAlignment( 0.5, costFunction ), new OptimalReadyToRunToNode() );
                break;
            default: {
                final String[] split = strategy.split( "-" );
                Prioritize prioritize;
                NodeAssign assign;
                if ( split.length <= 2 ) {
                    switch ( split[0].toLowerCase() ) {
                        case "fifo": prioritize = new FifoPrioritize(); break;
                        case "rank": prioritize = new RankPrioritize(); break;
                        case "rank_min": prioritize = new RankMinPrioritize(); break;
                        case "rank_max": prioritize = new RankMaxPrioritize(); break;
                        case "random": case "r": prioritize = new RandomPrioritize(); break;
                        case "max": prioritize = new MaxInputPrioritize(); break;
                        case "min": prioritize = new MinInputPrioritize(); break;
                        default:
                            return new ResponseEntity<>( "No Prioritize for: " + split[0], HttpStatus.NOT_FOUND );
                    }
                    if ( split.length == 2 ) {
                        switch ( split[1].toLowerCase() ) {
                            case "random": case "r": assign = new RandomNodeAssign(); break;
                            case "roundrobin": case "rr": assign = new RoundRobinAssign(); break;
                            case "fair": case "f": assign = new FairAssign(); break;
                            default:
                                return new ResponseEntity<>( "No Assign for: " + split[1], HttpStatus.NOT_FOUND );
                        }
                    } else {
                        assign = new RoundRobinAssign();
                    }
                    scheduler = new PrioritizeAssignScheduler( execution, client, namespace, config, prioritize, assign );
                } else {
                    return new ResponseEntity<>( "No scheduler for strategy: " + strategy, HttpStatus.NOT_FOUND );
                }
            }
        }

        schedulerHolder.put( execution, scheduler );
        client.addInformable( scheduler );

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Register a task for execution")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Task successfully registered",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    /**
     * Register a task for the execution
     *
     * @param execution unique name of the execution
     * @param config The config contains the task name, input files, and optional task parameter the scheduler has to determine
     * @return Parameters the scheduler suggests for the task
     */
    @PostMapping("/v1/scheduler/{execution}/task/{id}")
    ResponseEntity<? extends Object> registerTask( @PathVariable String execution, @PathVariable int id, @RequestBody TaskConfig config ) {

        log.trace( execution + " " + config.getTask() + " got: " + config );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        scheduler.addTask( id, config );
        Map<String, Object> schedulerParams = scheduler.getSchedulerParams( config.getTask(), config.getName() );

        return new ResponseEntity<>( schedulerParams, HttpStatus.OK );

    }


    @Operation(summary = "Delete a task of execution")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Task successfully deleted",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Task not found",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    /**
     * Delete a task, only works if the batch of the task was closed and if no pod was yet submitted.
     * If a pod was submitted, delete the pod instead.
     *
     * @param execution
     * @param id
     * @return
     */
    @DeleteMapping("/v1/scheduler/{execution}/task/{id}")
    ResponseEntity<? extends Object> deleteTask( @PathVariable String execution, @PathVariable int id ) {

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        final boolean found = scheduler.removeTask( id );
        return new ResponseEntity<>( found ? HttpStatus.OK : HttpStatus.NOT_FOUND );

    }

    @Operation(summary = "")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully started batch",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @PutMapping("/v1/scheduler/{execution}/startBatch")
    ResponseEntity<String> startBatch( @PathVariable String execution ) {

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }
        scheduler.startBatch();
        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "End a batch")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully ended batch",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @PutMapping("/v1/scheduler/{execution}/endBatch")
    ResponseEntity<String> endBatch( @PathVariable String execution, @RequestBody int tasksInBatch ) {

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }
        scheduler.endBatch( tasksInBatch );
        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Check the state of a task")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Task found",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    /**
     * Check Task state
     *
     * @param execution unique name of the execution
     * @param id        unique id of task
     * @return boolean
     */
    @GetMapping("/v1/scheduler/{execution}/task/{id}")
    ResponseEntity<? extends Object> getTaskState( @PathVariable String execution, @PathVariable int id ) {

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        return new ResponseEntity<>( scheduler.getTaskState( id ), HttpStatus.OK );

    }

    @Operation(summary = "Delete an execution after it has finished or crashed")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Execution successfully deleted",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    /**
     * Call this after the execution has finished
     *
     * @param execution unique name of the execution
     * @return
     */
    @DeleteMapping("/v1/scheduler/{execution}")
    ResponseEntity<String> delete( @PathVariable String execution ) {

        log.info( "Delete scheduler: " + execution );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }
        schedulerHolder.remove( execution );
        client.removeInformable( scheduler );
        scheduler.close();
        closedLastScheduler = System.currentTimeMillis();
        return new ResponseEntity<>( HttpStatus.OK );
    }

    @GetMapping("/v1/daemon/{execution}/{node}")
    ResponseEntity<String> getDaemonName( @PathVariable String execution, @PathVariable String node ) {

        log.info( "Asking for Daemon exec: {} node: {}", execution, node );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( !(scheduler instanceof SchedulerWithDaemonSet) ) {
            return noSchedulerFor( execution );
        }

        String daemon = ((SchedulerWithDaemonSet) scheduler).getDaemonIpOnNode( node );

        if ( daemon == null ){
            return new ResponseEntity<>( "No daemon for node found: " + node , HttpStatus.NOT_FOUND );
        }

        return new ResponseEntity<>( daemon, HttpStatus.OK );

    }

    @PostMapping("/v1/downloadtask/{execution}")
    ResponseEntity<String> finishDownload( @PathVariable String execution, @RequestBody byte[] task ) {

        String name = new String( task, StandardCharsets.UTF_8 );
        if ( name.endsWith( "=" ) ) {
            name = name.substring( 0, name.length() - 1 );
        }

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( !(scheduler instanceof SchedulerWithDaemonSet) ) {
            return noSchedulerFor( execution );
        }
        ((SchedulerWithDaemonSet) scheduler).taskHasFinishedCopyTask(  name );
        return new ResponseEntity<>( HttpStatus.OK );

    }

    @GetMapping("/v1/file/{execution}")
    ResponseEntity<? extends Object> getNodeForFile( @PathVariable String execution, @RequestParam String path ) {

        log.info( "Get file location request: {} {}", execution, path );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( !(scheduler instanceof SchedulerWithDaemonSet) ) {
            return noSchedulerFor( execution );
        }

        FileResponse fileResponse;
        try {
            fileResponse = ((SchedulerWithDaemonSet) scheduler).nodeOfLastFileVersion( path );
            log.info(fileResponse.toString());
        } catch (NotARealFileException e) {
            return new ResponseEntity<>( "Requested path is not a real file: " + path , HttpStatus.BAD_REQUEST );
        }

        if ( fileResponse == null ){
            return new ResponseEntity<>( "No node for file found: " + path , HttpStatus.NOT_FOUND );
        }

        return new ResponseEntity<>( fileResponse, HttpStatus.OK );

    }

    @PostMapping("/v1/file/{execution}/location/{method}")
    ResponseEntity<String> changeLocationForFile( @PathVariable String method, @PathVariable String execution, @RequestBody PathAttributes pa ) {
        return changeLocationForFile( method, execution, null, pa );
    }

    @PostMapping("/v1/file/{execution}/location/{method}/{node}")
    ResponseEntity<String> changeLocationForFile( @PathVariable String method, @PathVariable String execution, @PathVariable String node, @RequestBody PathAttributes pa ) {

        log.info( "Change file location request: {} {} {}", method, execution, pa );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( !(scheduler instanceof SchedulerWithDaemonSet) ) {
            log.info( "No scheduler for: " + execution );
            return noSchedulerFor( execution );
        }

        if ( !method.equals("add") && !method.equals("overwrite") ) {
            log.info("Method not found: " + method);
            return new ResponseEntity<>( "Method not found: " + method , HttpStatus.NOT_FOUND );
        }

        boolean overwrite = method.equals("overwrite");

        ((SchedulerWithDaemonSet) scheduler).addFile( pa.getPath(), pa.getSize(), pa.getTimestamp(), pa.getLocationWrapperID(), overwrite, node );

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @GetMapping ("/health")
    ResponseEntity<Object> checkHealth() {
        return new ResponseEntity<>( HttpStatus.OK );
    }

    @Operation(summary = "Register DAG vertices")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Vertices successfully registered",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @PostMapping("/v1/scheduler/{execution}/DAG/vertices")
    ResponseEntity<String> addVertices( @PathVariable String execution, @RequestBody List<Vertex> vertices ) {

        log.trace( "submit vertices: {}", vertices );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        scheduler.getDag().registerVertices( vertices );

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Delete DAG vertices")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Vertices successfully removed",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @DeleteMapping("/v1/scheduler/{execution}/DAG/vertices")
    ResponseEntity<String> deleteVertices( @PathVariable String execution, @RequestBody int[] vertices ) {

        log.trace( "submit vertices: {}", vertices );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        scheduler.getDag().removeVertices( vertices );

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Register DAG edges")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Edges successfully registered",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @PostMapping("/v1/scheduler/{execution}/DAG/edges")
    ResponseEntity<String> addEdges( @PathVariable String execution, @RequestBody List<InputEdge> edges ) {

        log.trace( "submit edges: {}", edges );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        final DAG dag = scheduler.getDag();
        dag.registerEdges( edges );

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Delete DAG edges")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Edges successfully removed",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @DeleteMapping("/v1/scheduler/{execution}/DAG/edges")
    ResponseEntity<String> deleteEdges( @PathVariable String execution, @RequestBody int[] edges ) {

        log.trace( "submit edges: {}", edges );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        final DAG dag = scheduler.getDag();
        dag.removeEdges( edges );

        return new ResponseEntity<>( HttpStatus.OK );

    }

}
