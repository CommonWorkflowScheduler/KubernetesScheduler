package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.offset.VarianceOffset;
import cws.k8s.scheduler.prediction.offset.WeightedVarianceOffset;
import cws.k8s.scheduler.util.Formater;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This predictor uses Pearson to decide if the data is constant or linear.
 * If the data is linear, it uses a linear predictor, otherwise it uses the mean.
 */
@Slf4j
public class PonderingPredictorSpecialWeighted extends WeightedVarianceOffset implements Predictor {

    private final AtomicLong version = new AtomicLong( 0 );
    private final LinearPredictor linearPredictor;
    private Double r = null;
    double mean = 0;
    double max = 0;
    double min = Double.MAX_VALUE;
    double maxX = 0;
    double minX = Double.MAX_VALUE;
    long n = 0;
    private long fixedOffset = 1024L * 1024L * 128L;

    private final Task[] firstTasks;

    private final List<Task> observedTasks = new LinkedList<>();

    public PonderingPredictorSpecialWeighted( LinearPredictor predictor ) {
        this( predictor, 5 );
    }

    public PonderingPredictorSpecialWeighted( LinearPredictor predictor, int firstTasksSize ) {
        super( predictor);
        linearPredictor = predictor;
        firstTasks = new Task[firstTasksSize];
    }

    @Override
    public void addTask( Task t ) {
        if ( t == null ) {
            throw new IllegalArgumentException( "Task cannot be null" );
        }
        double output = getDependentValue( t );
        double input = getIndependentValue( t );
        synchronized ( linearPredictor ) {
            version.incrementAndGet();
            mean = ( mean * n + output ) / ( n + 1 );
            if ( output > max ) {
                max = output;
            }
            if ( output < min ) {
                min = output;
            }
            if ( n < firstTasks.length ) {
                firstTasks[(int) n] = t;
            }
            if ( input > maxX ) {
                maxX = input;
            }
            if ( input < minX ) {
                minX = input;
            }
            n++;
            linearPredictor.addTask( t );
            if ( n >= 2 ){
                r = linearPredictor.getR();
            }
            observedTasks.add( t );
        }
    }

    @Override
    public Double queryPrediction( Task task ) {
        synchronized ( linearPredictor ) {

            final double independentValue = linearPredictor.getIndependentValue( task );
            // If we have less than 5 tasks, predict using rules
            if ( n < firstTasks.length ) {
                return predictionForFirst5Tasks( independentValue );
            }

            if ( r == null ) {
                //Should not happen because we check for n before
                log.info( "Not enough data to predict" );
                return null;
            }

            //if positive linear relationship
            if ( r > 0.3 ) {
                Double prediction = predict( task );
                return checkPrediction( prediction, independentValue );
            }

            // if no relationship
            if ( r >= 0 ) {
                log.info( "Using Max predictor: {}", Formater.formatBytes( (long) max ) );
                return max + fixedOffset;
            }

            // if negative linear relationship
            log.info( "Using Max predictor: {}", Formater.formatBytes( (long) max ) );
            return max + fixedOffset;
        }
    }

    @Nullable
    private Double predictionForFirst5Tasks( double independentValue ) {
        for ( int i = 0; i < n; i++ ) {
            if ( linearPredictor.getIndependentValue( firstTasks[i] ) > independentValue ) {
                // Assuming that a larger input leads to a larger output
                return max + fixedOffset;
            }
        }
        return null;
    }

    private Double predict( Task task ) {
        return linearPredictor.queryPrediction( task );
    }

    @Nullable
    private Double checkPrediction( Double prediction, double independentValue ) {
        if ( prediction == null ) {
            return null;
        } else if ( prediction < min ) {
            log.info( "Using Min predictor: {}", Formater.formatBytes( (long) min ) );
            prediction = min;
        } else if ( prediction > max && independentValue < maxX || independentValue >= maxX && prediction < max ) {
            log.info( "Using Max predictor: {}", Formater.formatBytes( (long) max ) );
            prediction = max;
        } else {
            log.info( "Using Linear predictor: {}", Formater.formatBytes( (long) prediction.doubleValue() ) );
        }
        final double offset = 2 * Math.sqrt( determineOffset( independentValue ) );
        return applyOffset( prediction, Math.max( fixedOffset, offset ) );
    }

    private double applyOffset( double prediction, double offset ) {
        return prediction + offset;
    }

    @Override
    public double getDependentValue( Task task ) {
        return linearPredictor.getDependentValue( task );
    }

    public double getIndependentValue( Task task ) {
        return linearPredictor.getIndependentValue( task );
    }

    @Override
    public long getVersion() {
        return version.get();
    }

    private double determineOffset( double independentValue ) {
        return getOffset( observedTasks, independentValue );
    }

}
