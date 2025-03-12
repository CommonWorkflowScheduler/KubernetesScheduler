package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class WeightedVarianceOffset {

    private final Predictor predictor;

    public WeightedVarianceOffset( Predictor predictor ) {
        this.predictor = predictor;
    }

    /**
     * Get the offset for the given value
     * @param observedTasks
     * @param value, values further away from the value will have a lower weight
     * @return
     */
    protected double getOffset( List<Task> observedTasks, double value ) {
        double[] diffs = new double[observedTasks.size()];
        double[] independentValues = new double[observedTasks.size()];
        int n = observationsToDifferenceArray( observedTasks, diffs, independentValues );
        double[] weights = new double[n];
        calculateWeights( weights, independentValues, value );
        final double variance = calculateVariance( diffs, n, weights );
        log.info( "Variance: " + variance );
        return variance;
    }

    private void calculateWeights( double[] weights, double[] independentValues, double value ){

        // find maxDiff
        double maxDiff = 0;
        for (int i = 0; i < weights.length; i++) {
            final double diff = Math.abs( independentValues[i] - value );
            if ( diff > maxDiff ) {
                maxDiff = diff;
            }
        }
        if ( maxDiff == 0 ) {
            // all values are the same
            Arrays.fill( weights, 1 );
            return;
        }

        // do not weight values with 0 until we have at least 10 observations
        double extraTerm = Math.max( 1 - ( weights.length / 10.0 ), 0) / 100;

        for (int i = 0; i < weights.length; i++) {
            final double diff = Math.abs( independentValues[i] - value );
            weights[i] = 1 - (diff / maxDiff) + extraTerm;
        }

    }

    /**
     * Convert the observed tasks to an array of differences between the observed value and the prediction
     * @param observedTasks the observed tasks
     * @param diff the array to be filled with the differences
     * @return the number of differences added to the array
     */
    protected int observationsToDifferenceArray( List<Task> observedTasks, double[] diff, double[] independent ){
        int n = 0;
        for ( Task observedTask : observedTasks ) {
            final Double v = predictor.queryPrediction( observedTask );
            if ( v == null ) {
                continue;
            }
            independent[n] = predictor.getIndependentValue( observedTask );
            diff[n++] = predictor.getDependentValue( observedTask ) - v;
        }
        return n;
    }

    /**
     * Calculate the weighted variance of an array of values
     * The variance is calculated for the first n elements of the array
     * https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Weighted_sample_variance
     * @param values the array of values
     * @param n the number of elements to consider
     * @return the variance of the first n elements of the array
     */
    protected double calculateVariance( double[] values, int n, double[] weights ) {
        if ( n <= 1 ) {
            return 0;
        }
        if ( n > values.length ) {
            throw new IllegalArgumentException( "n cannot be greater than the length of the array" );
        }

        double v1 = 0;
        double v2 = 0;
        for (int i = 0; i < n; i++) {
            v1 += weights[i];
            v2 += weights[i] * weights[i];
        }

        if (v1 == 0) {
            throw new IllegalArgumentException("Sum of weights cannot be zero");
        }

        double mean = 0;
        for (int i = 0; i < n; i++) {
            mean += values[i] * weights[i];
        }
        mean /= v1;
        double variance = 0;
        for (int i = 0; i < n; i++) {
            final double diff = values[i] - mean;
            variance += diff * diff * weights[i];
        }
        variance /= v1 - (v2 / v1);
        return variance;
    }

}
