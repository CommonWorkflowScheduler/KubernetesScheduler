package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.extractor.VariableExtractor;
import cws.k8s.scheduler.util.Formater;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This predictor uses Pearson to decide if the data is constant or linear.
 * If the data is linear, it uses a linear predictor, otherwise it uses the mean.
 */
@Slf4j
public class PonderingPredictor implements Predictor {

    private final AtomicLong version = new AtomicLong( 0 );
    private final LinearPredictor linearPredictor;
    boolean isLinear = false;
    double mean = 0;
    long n = 0;

    public PonderingPredictor( LinearPredictor predictor ) {
        linearPredictor = predictor;
    }

    @Override
    public void addTask( Task t ) {
        if ( t == null ) {
            throw new IllegalArgumentException( "Task cannot be null" );
        }
        double output = getDependentValue( t );
        synchronized ( linearPredictor ) {
            version.incrementAndGet();
            mean = ( mean * n + output ) / ( n + 1 );
            n++;
            linearPredictor.addTask( t );
            if ( n >= 2 ){
                isLinear = linearPredictor.getR() > 0.3 || linearPredictor.getR() < -0.3;
            }
        }
    }

    @Override
    public Double queryPrediction( Task task ) {
        synchronized ( linearPredictor ) {
            if ( isLinear ) {
                final Double v = linearPredictor.queryPrediction( task );
                log.info( "Using linear predictor: {}", Formater.formatBytes( v.longValue() ) );
                return v;
            } else if ( n == 0 ) {
                return null;
            } else {
                log.info( "Using Mean predictor: {}", Formater.formatBytes( (long) mean ) );
                return mean;
            }
        }
    }

    @Override
    public double getDependentValue( Task task ) {
        return linearPredictor.getDependentValue( task );
    }

    @Override
    public long getVersion() {
        return version.get();
    }

}
