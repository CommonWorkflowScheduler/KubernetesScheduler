package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@RequiredArgsConstructor
public abstract class OffsetApplier implements Predictor {

    @Getter(AccessLevel.PROTECTED)
    private final Predictor predictor;
    private final List<Task> observedTasks = new LinkedList<>();
    private Double recentOffset = null;


    @Override
    public void addTask( Task t ) {
        synchronized ( this ) {
            predictor.addTask( t );
            observedTasks.add( t );
            recentOffset = null;
        }
    }

    @Override
    public Double queryPrediction( Task task ) {
        synchronized ( this ) {
            final Double prediction = predictor.queryPrediction( task );
            return prediction == null ? null : applyOffset( prediction, determineOffset() );
        }
    }

    @Override
    public double getDependentValue( Task task ) {
        return predictor.getDependentValue( task );
    }

    private double determineOffset() {
        if ( recentOffset == null ) {
            recentOffset = getOffset( observedTasks );
        }
        return recentOffset;
    }

    /**
     * Apply the offset to the prediction, by default the offset is added to the prediction
     * @param prediction the prediction to be offset
     * @return the offset prediction
     */
    protected double applyOffset( double prediction, double offset ) {
        return prediction + offset;
    }

    /**
     * Get the offset to be applied to the prediction
     * @return the offset
     */
    protected abstract double getOffset( List<Task> observedTasks );

    @Override
    public long getVersion() {
        return predictor.getVersion();
    }

    /**
     * Convert the observed tasks to an array of differences between the observed value and the prediction
     * @param observedTasks the observed tasks
     * @param array the array to be filled with the differences
     * @return the number of differences added to the array
     */
    protected int observationsToDifferenceArray( List<Task> observedTasks, double[] array ){
        int n = 0;
        for ( Task observedTask : observedTasks ) {
            final Double v = getPredictor().queryPrediction( observedTask );
            if ( v == null ) {
                continue;
            }
            array[n++] = getDependentValue( observedTask ) - v;
        }
        return n;
    }

}
