package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.extractor.VariableExtractor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This predictor uses Pearson to decide if the data is constant or linear.
 * If the data is linear, it uses a linear predictor, otherwise it uses the mean.
 */
@Slf4j
public class PonderingPredictor implements Predictor {

    private final AtomicLong version = new AtomicLong( 0 );
    private final VariableExtractor outputExtractor;
    private final LinearPredictor linearPredictor;
    boolean isLinear = false;
    double mean = 0;
    long n = 0;

    public PonderingPredictor( VariableExtractor inputExtractor, VariableExtractor outputExtractor ) {
        this.outputExtractor = outputExtractor;
        linearPredictor = new LinearPredictor( inputExtractor, outputExtractor );
    }

    @Override
    public void addTask( Task t ) {
        double output = outputExtractor.extractVariable( t );
        synchronized ( linearPredictor ) {
            version.incrementAndGet();
            mean = ( mean * n + output ) / ( n + 1 );
            n++;
            linearPredictor.addTask( t );
            isLinear = linearPredictor.getR() > 0.3 || linearPredictor.getR() < -0.3;
        }
    }

    @Override
    public Double queryPrediction( Task task ) {
        synchronized ( linearPredictor ) {
            if ( isLinear ) {
                return linearPredictor.queryPrediction( task );
            } else if ( n == 0 ) {
                return null;
            } else {
                return mean;
            }
        }
    }

    @Override
    public double getDependentValue( Task task ) {
        return outputExtractor.extractVariable( task );
    }

    @Override
    public long getVersion() {
        return version.get();
    }

}
