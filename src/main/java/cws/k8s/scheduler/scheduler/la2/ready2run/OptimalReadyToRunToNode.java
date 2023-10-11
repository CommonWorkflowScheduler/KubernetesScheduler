package cws.k8s.scheduler.scheduler.la2.ready2run;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.scheduler.data.TaskInputsNodes;
import cws.k8s.scheduler.util.LogCopyTask;
import cws.k8s.scheduler.util.NodeTaskLocalFilesAlignment;
import cws.k8s.scheduler.util.score.CalculateScore;
import com.google.ortools.Loader;
import com.google.ortools.sat.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class OptimalReadyToRunToNode implements ReadyToRunToNode {

    private final BigDecimal MILLION = new BigDecimal(1_000_000);
    private CalculateScore calculateScore = null;

    @Setter
    private LogCopyTask logger;

    @Override
    public void init( CalculateScore calculateScore ) {
        Loader.loadNativeLibraries();
        this.calculateScore = calculateScore;
    }

    /**
     * Creates an optimal alignment for tasks with all data on node.
     */
    @Override
    public List<NodeTaskLocalFilesAlignment> createAlignmentForTasksWithAllDataOnNode(
            List<TaskInputsNodes> taskWithAllData,
            Map<NodeWithAlloc, Requirements> availableByNode
    ) {

        if ( taskWithAllData.isEmpty() || availableByNode.isEmpty() ){
            log.info( "No tasks can be scheduled on any node. (No node has all data)" );
            return Collections.emptyList();
        }

        long start = System.currentTimeMillis();
        final LinkedList<TaskNodeBoolVar> taskNodeBoolVars = new LinkedList<>();

        CpModel model = new CpModel();

        Map<NodeWithAlloc,LinearExprBuilder> memUsed = new HashMap<>();
        Map<NodeWithAlloc,LinearExprBuilder> cpuUsed = new HashMap<>();
        for ( NodeWithAlloc node : availableByNode.keySet() ) {
            memUsed.put( node, LinearExpr.newBuilder() );
            cpuUsed.put( node, LinearExpr.newBuilder() );
        }

        LinearExprBuilder objective = LinearExpr.newBuilder();

        int index = 0;
        for ( TaskInputsNodes taskInputsNodes : taskWithAllData ) {
            List<Literal> onlyOnOneNode = new ArrayList<>();
            final long score = calculateScore.getScore( taskInputsNodes.getTask(), taskInputsNodes.getTaskSize() );
            final Requirements request = taskInputsNodes.getTask().getRequest();
            final long ram = request.getRam().longValue();
            final long cpu = request.getCpu().multiply( MILLION ).longValue();
            for ( NodeWithAlloc node : taskInputsNodes.getNodesWithAllData() ) {
                final Requirements availableOnNode = availableByNode.get( node );
                //Can schedule task on node?
                if ( availableOnNode != null && availableOnNode.higherOrEquals( request ) ) {
                        final BoolVar boolVar = model.newBoolVar( "x_" + index + "_" + node );
                        onlyOnOneNode.add( boolVar );
                        memUsed.get(node).addTerm( boolVar, ram );
                        cpuUsed.get(node).addTerm( boolVar, cpu );
                        objective.addTerm( boolVar, score );
                        taskNodeBoolVars.add( new TaskNodeBoolVar( taskInputsNodes, node, boolVar ) );
                }
            }
            if ( !onlyOnOneNode.isEmpty() ) {
                model.addAtMostOne(onlyOnOneNode);
            }
            index++;
        }

        if ( taskNodeBoolVars.isEmpty() ) {
            log.info( "No tasks can be scheduled on any node. Not enough resources available." );
            return Collections.emptyList();
        }

        for ( Map.Entry<NodeWithAlloc, LinearExprBuilder> entry : memUsed.entrySet() ) {
            model.addLessOrEqual( entry.getValue(), availableByNode.get( entry.getKey() ).getRam().longValue() );
            model.addLessOrEqual( cpuUsed.get( entry.getKey() ), availableByNode.get( entry.getKey() ).getCpu().multiply( MILLION ).longValue() );
        }

        log.info( "Model created in " + (System.currentTimeMillis() - start) + "ms ( " + taskNodeBoolVars.size() + " vars )" );
        model.maximize( objective );
        CpSolver solver = new CpSolver();
        solver.getParameters().setMaxTimeInSeconds( 10 );
        final CpSolverStatus solve = solver.solve( model );
        final String message = "Solved in " + (System.currentTimeMillis() - start) + "ms ( " + taskNodeBoolVars.size() + " vars ) + solution is: " + solve;
        log.info( message );
        logger.log( message );
        log.info("Total packed value: " + solver.objectiveValue());
        if ( solve != CpSolverStatus.OPTIMAL && solve != CpSolverStatus.FEASIBLE ) {
            return Collections.emptyList();
        }

        return taskNodeBoolVars.stream()
                .filter( taskNodeBoolVar -> solver.booleanValue( taskNodeBoolVar.getBoolVar() ) )
                .map( TaskNodeBoolVar::createAlignment )
                .collect( Collectors.toList() );
    }

    @Getter
    @RequiredArgsConstructor
    private class TaskNodeBoolVar {
        private final TaskInputsNodes taskInputsNodes;
        private final NodeWithAlloc node;
        private final BoolVar boolVar;

        NodeTaskLocalFilesAlignment createAlignment(){
            return new NodeTaskLocalFilesAlignment(
                    node,
                    taskInputsNodes.getTask(),
                    taskInputsNodes.getInputsOfTask().getSymlinks(),
                    taskInputsNodes.getInputsOfTask().allLocationWrapperOnLocation( node.getNodeLocation() )
            );
        }

    }

}
