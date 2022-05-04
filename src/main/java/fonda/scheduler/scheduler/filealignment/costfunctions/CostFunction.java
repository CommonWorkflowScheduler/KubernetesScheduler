package fonda.scheduler.scheduler.filealignment.costfunctions;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.util.AlignmentWrapper;

public interface CostFunction {

    double getInitCost();
    double getCost( LocationWrapper fileToTest );

    default double calculateCost(AlignmentWrapper alignment, LocationWrapper fileToTest ) {
        double currentCost = alignment == null || alignment.empty() ? getInitCost() : alignment.getCost();
        return currentCost + getCost( fileToTest );
    }


}
