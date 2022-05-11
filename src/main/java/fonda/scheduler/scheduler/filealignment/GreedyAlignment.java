package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.costfunctions.CostFunction;
import fonda.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import fonda.scheduler.util.*;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class GreedyAlignment implements InputAlignment {

    private final CostFunction cf;

    public GreedyAlignment(CostFunction cf) {
        this.cf = cf;
    }

    /**
     * Check if another scheduled task is already copying a required file
     *
     * @param currentlyCopying
     * @param path
     * @param locations
     * @return
     */
    private Tuple<LocationWrapper,Task> alreadyCopying(Map<String, Tuple<Task, Location>> currentlyCopying, String path, List<LocationWrapper> locations, String debug ){
        if (  currentlyCopying != null && currentlyCopying.containsKey( path )  ){
            final Tuple<Task, Location> taskLocationTuple = currentlyCopying.get(path);
            final Location copyFrom = taskLocationTuple.getB();
            for ( LocationWrapper locationWrapper : locations ) {
                if ( locationWrapper.getLocation() == copyFrom ) {
                    return new Tuple(locationWrapper,taskLocationTuple.getA());
                }
            }
            throw new NoAligmentPossibleException( "Node is a already copying file: " + path + " but in an incompatible version." );
        }
        return null;
    }

    private double findAlignmentForFile (
            PathFileLocationTriple pathFileLocationTriple,
            NodeLocation nodeLocation,
            HashMap<Location, AlignmentWrapper> map
    ){
        double minCost = Double.MAX_VALUE;
        Location bestLoc = null;
        LocationWrapper bestLocationWrapper = null;
        for (LocationWrapper locationWrapper : pathFileLocationTriple.locations) {
            final Location currentLoc = locationWrapper.getLocation();
            final double calculatedCost;
            if ( currentLoc != nodeLocation ) {
                final AlignmentWrapper alignmentWrapper = map.get(currentLoc);
                calculatedCost = cf.calculateCost(alignmentWrapper, locationWrapper);
            } else {
                calculatedCost = 0;
            }
            if ( calculatedCost < minCost ) {
                bestLoc = currentLoc;
                minCost = calculatedCost;
                bestLocationWrapper = locationWrapper;
            }
        }
        final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(bestLoc, k -> new AlignmentWrapper() );
        alignmentWrapper.addAlignmentToCopy( new FilePath( pathFileLocationTriple, bestLocationWrapper ), minCost );
        return minCost;
    }

    private boolean canUseFileFromOtherTask (
        Map<String, Tuple<Task, Location>> currentlyCopying,
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
            alignmentWrapper.addAlignmentToWaitFor(new FilePathWithTask(pathFileLocationTriple, copying.getA(), copying.getB()));
            return true;
        }
        return false;
    }

    @Override
    public FileAlignment getInputAlignment(@NotNull Task task,
                                           @NotNull TaskInputs inputsOfTask,
                                           @NotNull NodeWithAlloc node,
                                           Map<String, Tuple<Task, Location>> currentlyCopying,
                                           Map<String, Tuple<Task, Location>> currentlyPlanedToCopy,
                                           double maxCost) {
        inputsOfTask.sort();
        final HashMap<Location, AlignmentWrapper> map = new HashMap<>();
        double cost = 0;
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            if ( !canUseFileFromOtherTask( currentlyCopying, pathFileLocationTriple, map, "copying" )
                    &&
                    ! canUseFileFromOtherTask( currentlyPlanedToCopy, pathFileLocationTriple, map, "currentSchedule" )
            ) {
                    final double newCost = findAlignmentForFile( pathFileLocationTriple, node.getNodeLocation(), map );
                    if ( newCost > maxCost ) return null;
                    if ( newCost > cost ) cost = newCost;
            }
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), cost);
    }

}
