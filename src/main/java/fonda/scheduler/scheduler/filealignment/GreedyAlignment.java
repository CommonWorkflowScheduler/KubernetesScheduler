package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.costfunctions.CostFunction;
import fonda.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import fonda.scheduler.util.AlignmentWrapper;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.FilePath;
import fonda.scheduler.util.Tuple;
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
     * @param currentlyCopying
     * @param path
     * @param locations
     * @return
     */
    private LocationWrapper alreadyCopying(Map<String, Tuple<Task, Location>> currentlyCopying, String path, List<LocationWrapper> locations){
        if (  currentlyCopying != null && currentlyCopying.containsKey( path )  ){
            final Location copyFrom = currentlyCopying.get(path).getB();
            for ( LocationWrapper locationWrapper : locations ) {
                if ( locationWrapper.getLocation() == copyFrom ) {
                    return locationWrapper;
                }
            }
            throw new NoAligmentPossibleException( "Node is a already copying file: " + path + " but in an incompatible version." );
        }
        return null;
    }

    @Override
    public FileAlignment getInputAlignment(@NotNull Task task,
                                           @NotNull TaskInputs inputsOfTask,
                                           @NotNull NodeWithAlloc node,
                                           Map<String, Tuple<Task, Location>> currentlyCopying,
                                           double maxCost) {
        inputsOfTask.sort();
        final HashMap<Location, AlignmentWrapper> map = new HashMap<>();
        double cost = 0;
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            double minCost = Double.MAX_VALUE;
            Location bestLoc = null;
            LocationWrapper bestLocationWrapper = null;

            final LocationWrapper copying = alreadyCopying(
                    currentlyCopying,
                    pathFileLocationTriple.path.toString(),
                    pathFileLocationTriple.locations
            );
            if ( copying != null ) {
                minCost = 0;
                bestLoc = copying.getLocation();
                bestLocationWrapper = copying;
            } else {
                for (LocationWrapper locationWrapper : pathFileLocationTriple.locations) {
                    final Location currentLoc = locationWrapper.getLocation();
                    final double calculatedCost;
                    if ( currentLoc != node.getNodeLocation() ) {
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
                if ( minCost > maxCost ) return null;
                if ( minCost > cost ) cost = minCost;
            }
            final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(bestLoc, k -> new AlignmentWrapper() );
            alignmentWrapper.addAlignment( new FilePath( pathFileLocationTriple, bestLocationWrapper ), minCost );
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), cost);
    }

}
