package cws.k8s.scheduler.prediction.predictor.loss;

import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.analysis.MultivariateFunction;

import java.util.ArrayList;

@RequiredArgsConstructor
public class UnequalLossFunction implements MultivariateFunction {
    private final ArrayList<Double> x = new ArrayList<>();
    private final ArrayList<Double> y = new ArrayList<>();

    /**
     * Lambda parameter for the loss function to penalize overpredictions less
     */
    private final double lambda;

    public void addPoint(double x, double y) {
        this.x.add( x );
        this.y.add( y );
    }

    @Override
    public double value(double[] parameters) {
        double cLoss = 0.0;
        for (int i = 0; i < x.size(); i++) {
            cLoss += getLoss( parameters, i );
        }
        return cLoss;
    }

    private double getLoss( double[] parameters, int i ) {
        final Double x1 = x.get( i );
        double predicted = parameters[0] + x1 * parameters[1];
        double error = y.get( i ) - predicted;
        double loss;
        if (error > 0) {
            loss = error * error; // Penalize underpredictions more
        } else {
            loss = lambda * error * error; // Penalize overpredictions less
        }
        return loss;
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