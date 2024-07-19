package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;

import java.util.List;

public class StandardDeviationOffset extends VarianceOffset {

    private final double factor;

    public StandardDeviationOffset( double factor, Predictor predictor ) {
        super( predictor );
        this.factor = factor;
    }

    @Override
    public double getOffset( List<Task> observedTasks ) {
        final double offset = super.getOffset( observedTasks );
        return factor * Math.sqrt( offset );
    }

}
