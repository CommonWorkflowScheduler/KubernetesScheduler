package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;

import java.util.List;

public class MaxOffset extends OffsetApplier {

    public MaxOffset( Predictor predictor ) {
        super( predictor );
    }

    @Override
    protected double getOffset( List<Task> observedTasks ) {
        double maxDiff = 0;
        for ( Task observedTask : observedTasks ) {
            final Double v = getPredictor().queryPrediction( observedTask );
            if (v == null) {
                continue;
            }
            double diff = getDependentValue( observedTask ) - v;
            if (diff > maxDiff) {
                maxDiff = diff;
            }
        }
        return maxDiff;
    }
}
