package cws.k8s.scheduler.scheduler.filealignment;

import cws.k8s.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.taskinputs.PathFileLocationTriple;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.util.AlignmentWrapper;
import cws.k8s.scheduler.util.FileAlignment;
import cws.k8s.scheduler.util.FilePathWithTask;
import cws.k8s.scheduler.util.Tuple;
import cws.k8s.scheduler.util.copying.CopySource;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
abstract class InputAlignmentClass implements InputAlignment {

    /**
     * This weight weights the cost of copying files from one node. 1 - this value weights the overall data copied.
     * How much should the individual costs of one node be weighted compared to the overall data copied.
     */
    private final double weightForIndividualNode;

    public InputAlignmentClass( double weightForIndividualNode ) {
        if ( weightForIndividualNode < 0 || weightForIndividualNode > 1 ) {
            throw new IllegalArgumentException( "Weight for single source must be between 0 and 1" );
        }
        this.weightForIndividualNode = weightForIndividualNode;
    }

    /**
     * Check if another scheduled task is already copying a required file
     *
     */
    private Tuple<LocationWrapper,Task> alreadyCopying( CurrentlyCopyingOnNode currentlyCopying, String path, List<LocationWrapper> locations, String debug ){
        if ( currentlyCopying != null && currentlyCopying.isCurrentlyCopying( path )  ){
            final CopySource copySource = currentlyCopying.getCopySource( path );
            final Location copyFrom = copySource.getLocation();
            for ( LocationWrapper locationWrapper : locations ) {
                if ( locationWrapper.getLocation() == copyFrom ) {
                    return new Tuple<>(locationWrapper,copySource.getTask());
                }
            }
            throw new NoAligmentPossibleException( "Node is a already copying file: " + path + " but in an incompatible version." );
        }
        return null;
    }

    private boolean canUseFileFromOtherTask (
            CurrentlyCopyingOnNode currentlyCopying,
            PathFileLocationTriple pathFileLocationTriple,
            Map<Location, AlignmentWrapper> map,
            String debug
    ) {
        final Tuple<LocationWrapper, Task> copying = alreadyCopying(
                currentlyCopying,
                pathFileLocationTriple.path.toString(),
                pathFileLocationTriple.locations,
                debug
        );
        if ( copying != null ) {
            final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(copying.getA().getLocation(), k -> new AlignmentWrapper());
            alignmentWrapper.addAlignmentToWaitFor(new FilePathWithTask(pathFileLocationTriple, copying.getA(), copying.getB()), copying.getA().getSizeInBytes());
            return true;
        }
        return false;
    }

    @Override
    public FileAlignment getInputAlignment(@NotNull Task task,
                                           @NotNull TaskInputs inputsOfTask,
                                           @NotNull NodeWithAlloc node,
                                           CurrentlyCopyingOnNode currentlyCopying,
                                           CurrentlyCopyingOnNode currentlyPlanedToCopy,
                                           double maxCost) {
        final HashMap<Location, AlignmentWrapper> map = new HashMap<>();
        Costs costs = new Costs();
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            if ( !canUseFileFromOtherTask( currentlyCopying, pathFileLocationTriple, map, "copying" )
                    &&
                    !canUseFileFromOtherTask( currentlyPlanedToCopy, pathFileLocationTriple, map, "currentSchedule" )
            ) {
                costs = findAlignmentForFile( pathFileLocationTriple, node.getNodeLocation(), map, costs );
                if ( costs.calculatedCost > maxCost ) {
                    return null;
                }
            }
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), costs.calculatedCost);
    }

    protected double calculateCost( double maxIndividual, double sumOfCost ) {
        return maxIndividual * weightForIndividualNode + sumOfCost * ( 1 - weightForIndividualNode );
    }

    abstract Costs findAlignmentForFile(
            PathFileLocationTriple pathFileLocationTriple,
            NodeLocation scheduledNode,
            HashMap<Location, AlignmentWrapper> map,
            final Costs costs
    );

    @Getter
    @RequiredArgsConstructor
    class Costs {

        private final double maxCostForIndividualNode;
        private final double sumOfCosts;
        private final double calculatedCost;

        public Costs() {
            this( 0, 0, 0 );
        }
    }

}
