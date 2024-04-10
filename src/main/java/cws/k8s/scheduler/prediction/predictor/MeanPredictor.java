package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.extractor.VariableExtractor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
public class MeanPredictor implements Predictor {

    private final AtomicLong version = new AtomicLong( 0 );
    private final VariableExtractor outputExtractor;
    private double mean = 0;
    private int count = 0;


    @Override
    public void addTask( Task t ) {
        if ( t == null ) {
            throw new IllegalArgumentException( "Task cannot be null" );
        }
        double output = outputExtractor.extractVariable( t );
        synchronized ( this ) {
            version.incrementAndGet();
            mean = ( mean * count + output ) / ( count + 1 );
        }
    }

    @Override
    public Double queryPrediction( Task task ) {
        return mean == 0 ? null : mean;
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
