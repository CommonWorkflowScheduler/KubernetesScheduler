package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.costfunctions.CostFunction;
import fonda.scheduler.util.AlignmentWrapper;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.FilePath;
import fonda.scheduler.util.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class GreedyAlignment extends InputAlignmentClass {

    private final CostFunction cf;

    public GreedyAlignment(CostFunction cf) {
        this.cf = cf;
    }

    double findAlignmentForFile (
            PathFileLocationTriple pathFileLocationTriple,
            NodeLocation scheduledNode,
            HashMap<Location, AlignmentWrapper> map
    ){
        double minCost = Double.MAX_VALUE;
        Location bestLoc = null;
        LocationWrapper bestLocationWrapper = null;
        for (LocationWrapper locationWrapper : pathFileLocationTriple.locations) {
            final Location currentLoc = locationWrapper.getLocation();
            final double calculatedCost;
            if ( currentLoc != scheduledNode) {
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

    @Override
    public FileAlignment getInputAlignment(@NotNull Task task,
                                           @NotNull TaskInputs inputsOfTask,
                                           @NotNull NodeWithAlloc node,
                                           Map<String, Tuple<Task, Location>> currentlyCopying,
                                           Map<String, Tuple<Task, Location>> currentlyPlanedToCopy,
                                           double maxCost) {
        inputsOfTask.sort();
        return super.getInputAlignment( task, inputsOfTask, node, currentlyCopying, currentlyPlanedToCopy, maxCost );
    }

}
