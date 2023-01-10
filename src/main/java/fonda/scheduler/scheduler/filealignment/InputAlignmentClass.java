package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import fonda.scheduler.util.AlignmentWrapper;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.FilePathWithTask;
import fonda.scheduler.util.Tuple;
import fonda.scheduler.util.copying.CopySource;
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
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
    private final double weightForSingleSource;

    public InputAlignmentClass( double weightForSingleSource ) {
        assert weightForSingleSource >= 0 && weightForSingleSource <= 1;
        this.weightForSingleSource = weightForSingleSource;
    }

    /**
     * Check if another scheduled task is already copying a required file
     *
     * @param currentlyCopying
     * @param path
     * @param locations
     * @return
     */
    private Tuple<LocationWrapper,Task> alreadyCopying( CurrentlyCopyingOnNode currentlyCopying, String path, List<LocationWrapper> locations, String debug ){
        if ( currentlyCopying != null && currentlyCopying.isCurrentlyCopying( path )  ){
            final CopySource copySource = currentlyCopying.getCopySource( path );
            final Location copyFrom = copySource.getLocation();
            for ( LocationWrapper locationWrapper : locations ) {
                if ( locationWrapper.getLocation() == copyFrom ) {
                    return new Tuple(locationWrapper,copySource.getTask());
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
        double cost = 0;
        double overallCost = 0;
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            if ( !canUseFileFromOtherTask( currentlyCopying, pathFileLocationTriple, map, "copying" )
                    &&
                    !canUseFileFromOtherTask( currentlyPlanedToCopy, pathFileLocationTriple, map, "currentSchedule" )
            ) {
                final Costs result = findAlignmentForFile( pathFileLocationTriple, node.getNodeLocation(), map );
                final double newCost = (result.singleSourceCost * weightForSingleSource)
                        + (result.individualCost + overallCost ) * ( 1 - weightForSingleSource);
                if ( newCost > maxCost ) {
                    return null;
                }
                if ( newCost > cost ) {
                    cost = newCost;
                    overallCost += result.individualCost;
                }
            }
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), cost);
    }

    abstract Costs findAlignmentForFile(PathFileLocationTriple pathFileLocationTriple, NodeLocation scheduledNode, HashMap<Location, AlignmentWrapper> map);

    @RequiredArgsConstructor
    class Costs {

        private final double individualCost;
        private final double singleSourceCost;

    }

}
