package cws.k8s.scheduler.scheduler.filealignment.costfunctions;

import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.util.AlignmentWrapper;

public interface CostFunction {

    double getInitCost();
    double getCost( LocationWrapper fileToTest );

    default double calculateCost(AlignmentWrapper alignment, LocationWrapper fileToTest ) {
        double currentCost = alignment == null || alignment.empty() ? getInitCost() : alignment.getCost();
        return currentCost + getCost( fileToTest );
    }


}
