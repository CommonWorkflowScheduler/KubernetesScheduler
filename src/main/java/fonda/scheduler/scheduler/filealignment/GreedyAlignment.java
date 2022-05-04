package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.costfunctions.CostFunction;
import fonda.scheduler.util.AlignmentWrapper;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.FilePath;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;

public class GreedyAlignment implements InputAlignment {

    private final CostFunction cf;

    public GreedyAlignment(CostFunction cf) {
        this.cf = cf;
    }

    @Override
    public FileAlignment getInputAlignment(@NotNull Task task, @NotNull TaskInputs inputsOfTask, @NotNull NodeWithAlloc node, double maxCost) {
        inputsOfTask.sort();
        final HashMap<Location, AlignmentWrapper> map = new HashMap<>();
        double cost = 0;
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            double minCost = Double.MAX_VALUE;
            Location bestLoc = null;
            LocationWrapper bestLocationWrapper = null;
            for (LocationWrapper locationWrapper : pathFileLocationTriple.locations) {
                final Location currentLoc = locationWrapper.getLocation();
                final AlignmentWrapper alignmentWrapper = map.get(currentLoc);
                final double calculatedCost = cf.calculateCost(alignmentWrapper, locationWrapper);
                if ( calculatedCost < minCost ) {
                    bestLoc = currentLoc;
                    minCost = calculatedCost;
                    bestLocationWrapper = locationWrapper;
                }
            }
            if ( minCost > maxCost ) return null;
            if ( minCost > cost ) cost = minCost;
            final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(bestLoc, k -> new AlignmentWrapper() );
            alignmentWrapper.addAlignment( new FilePath( pathFileLocationTriple, bestLocationWrapper ), minCost );
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), cost);
    }

}
