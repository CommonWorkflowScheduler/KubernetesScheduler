package cws.k8s.scheduler.scheduler.filealignment.costfunctions;

import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MinSizeCost implements CostFunction {

    private final double initCost;

    @Override
    public double getInitCost() {
        return initCost;
    }

    @Override
    public double getCost(LocationWrapper fileToTest) {
        return fileToTest.getSizeInBytes();
    }

}
