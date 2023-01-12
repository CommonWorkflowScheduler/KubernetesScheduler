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
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;

@Slf4j
public class GreedyAlignment extends InputAlignmentClass {

    private final CostFunction cf;

    public GreedyAlignment( double weightForSingleSource, CostFunction cf ) {
        super( weightForSingleSource );
        this.cf = cf;
    }

    Costs findAlignmentForFile (
            PathFileLocationTriple pathFileLocationTriple,
            NodeLocation scheduledNode,
            HashMap<Location, AlignmentWrapper> map,
            final Costs costs
    ){
        double minCost = Double.MAX_VALUE;
        double currentMaxCostForIndividualNode = costs.getMaxCostForIndividualNode();
        double currentSumOfCosts = costs.getSumOfCosts();
        Location bestLoc = null;
        LocationWrapper bestLocationWrapper = null;
        for (LocationWrapper locationWrapper : pathFileLocationTriple.locations) {
            final Location currentLoc = locationWrapper.getLocation();
            final double maxCostOnIndividualNode;
            final double tmpCurrentSumOfCosts;
            if ( currentLoc != scheduledNode) {
                final AlignmentWrapper alignmentWrapper = map.get(currentLoc);
                tmpCurrentSumOfCosts = costs.getSumOfCosts() + cf.getCost( locationWrapper );
                final double costOnIndividualNode = cf.calculateCost(alignmentWrapper, locationWrapper);
                maxCostOnIndividualNode = Math.max( costOnIndividualNode, costs.getMaxCostForIndividualNode() );
            } else {
                tmpCurrentSumOfCosts = costs.getSumOfCosts();
                maxCostOnIndividualNode = costs.getMaxCostForIndividualNode();
            }
            final double calculatedCost = calculateCost( maxCostOnIndividualNode, tmpCurrentSumOfCosts );
            if ( calculatedCost < minCost ) {
                bestLoc = currentLoc;
                minCost = calculatedCost;
                bestLocationWrapper = locationWrapper;
                currentMaxCostForIndividualNode = maxCostOnIndividualNode;
                currentSumOfCosts = tmpCurrentSumOfCosts;
            }
        }
        final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(bestLoc, k -> new AlignmentWrapper() );
        alignmentWrapper.addAlignmentToCopy( new FilePath( pathFileLocationTriple, bestLocationWrapper ), minCost, bestLocationWrapper.getSizeInBytes() );
        return new Costs( currentMaxCostForIndividualNode, currentSumOfCosts, minCost );
    }

    @Override
    public FileAlignment getInputAlignment(@NotNull Task task,
                                           @NotNull TaskInputs inputsOfTask,
                                           @NotNull NodeWithAlloc node,
                                           CurrentlyCopyingOnNode currentlyCopying,
                                           CurrentlyCopyingOnNode currentlyPlanedToCopy,
                                           double maxCost) {
        inputsOfTask.sort();
        return super.getInputAlignment( task, inputsOfTask, node, currentlyCopying, currentlyPlanedToCopy, maxCost );
    }

}
