package fonda.scheduler.scheduler.filealignment.costfunctions;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.util.AlignmentWrapper;
import fonda.scheduler.util.FilePath;

import java.util.List;

public class MinSizeCost implements CostFunction {

    private final double initCost;

    public MinSizeCost(double initCost) {
        this.initCost = initCost;
    }

    @Override
    public double getInitCost() {
        return initCost;
    }

    @Override
    public double getCost(LocationWrapper fileToTest) {
        return fileToTest.getSizeInBytes();
    }

}
