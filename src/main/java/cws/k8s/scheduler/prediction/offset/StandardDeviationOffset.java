package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;

import java.util.List;

public class StandardDeviationOffset extends VarianceOffset {

    public StandardDeviationOffset( Predictor predictor ) {
        super( predictor );
    }

    @Override
    public double getOffset( List<Task> observedTasks ) {
        return Math.sqrt( super.getOffset( observedTasks ) );
    }

}
