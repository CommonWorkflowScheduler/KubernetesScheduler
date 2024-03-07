package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.List;

public class PercentileOffset extends OffsetApplier {

    private final Percentile percentile = new Percentile();
    private final double percentileValue;

    public PercentileOffset( Predictor predictor, double percentileValue ) {
        super( predictor );
        if ( percentileValue < 0 ) {
            throw new AssertionError( "Percentile value must be greater than 0" );
        }
        if ( percentileValue > 100 ) {
            throw new AssertionError( "Percentile value must be less than 100" );
        }
        this.percentileValue = percentileValue;
    }

    @Override
    protected double getOffset( List<Task> observedTasks ) {
        double[] observedValues = new double[observedTasks.size()];
        int i = 0;
        for ( Task observedTask : observedTasks ) {
            final Double v = getPredictor().queryPrediction( observedTask );
            if ( v == null ) {
                continue;
            }
            observedValues[i++] = getDependentValue( observedTask ) - v;
        }
        return Math.max( 0, percentile.evaluate( observedValues, 0, i, percentileValue ) );
    }
}
