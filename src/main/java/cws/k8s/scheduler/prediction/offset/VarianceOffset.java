package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.List;

public class VarianceOffset extends OffsetApplier {

    public VarianceOffset( Predictor predictor ) {
        super( predictor );
    }

    @Override
    protected double getOffset( List<Task> observedTasks ) {
        double[] observedValues = new double[observedTasks.size()];
        int n = observationsToDifferenceArray( observedTasks, observedValues );
        return calculateVariance( observedValues, n );
    }

    /**
     * Calculate the variance of an array of values
     * The variance is calculated for the first n elements of the array
     * @param values the array of values
     * @param n the number of elements to consider
     * @return the variance of the first n elements of the array
     */
    protected double calculateVariance( double[] values, int n ) {
        assert n > 1 && n <= values.length;
        double mean = 0;
        for (int i = 0; i < n; i++) {
            mean += values[i];
        }
        mean /= n;
        double variance = 0;
        for (int i = 0; i < n; i++) {
            final double diff = values[i] - mean;
            variance += diff * diff;
        }
        variance /= n - 1;
        return variance;
    }
}
