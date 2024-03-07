package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;

import java.util.List;

public class ZeroOffset extends OffsetApplier {

    public ZeroOffset( Predictor predictor ) {
        super( predictor );
    }

    @Override
    protected double getOffset( List<Task> observedTasks ) {
        return 0;
    }
}
