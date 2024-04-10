package cws.k8s.scheduler.prediction.predictor.loss;

import lombok.Getter;
import org.apache.commons.math3.analysis.MultivariateFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class UnequalLossFunction implements MultivariateFunction {
    private final ArrayList<Double> x = new ArrayList<>();
    private final ArrayList<Double> y = new ArrayList<>();

    public void addPoint(double x, double y) {
        this.x.add( x );
        this.y.add( y );
    }


    @Override
    public double value(double[] parameters) {
        double cLoss = 0.0;
        for (int i = 0; i < x.size(); i++) {
            double predicted = parameters[0] + x.get( i ) * parameters[1];
            double error = y.get( i ) - predicted;
            if (error > 0) {
                cLoss += error * error; // Penalize underpredictions more
            } else {
                cLoss += 0.1  * error * error; // Penalize overpredictions less
            }
        }
        return cLoss;
    }

    public boolean canTrain(){
        if (x.size() < 2) {
            return false;
        }
        double x0 = x.get(0);
        for (int i = 1; i < x.size(); i++) {
            if (x.get(i) != x0) {
                return true;
            }
        }
        return false;
    }

    public String toString() {
        return "x = " + x + "\n" + "y = " + y + "\n";
    }
}