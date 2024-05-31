package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.offset.VarianceOffset;
import cws.k8s.scheduler.util.Formater;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This predictor uses Pearson to decide if the data is constant or linear.
 * If the data is linear, it uses a linear predictor, otherwise it uses the mean.
 */
@Slf4j
public class PonderingPredictorSpecial extends VarianceOffset implements Predictor {

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

    private final Task[] firstTasks = new Task[5];

    public PonderingPredictorSpecial( LinearPredictor predictor ) {
        super( predictor);
        linearPredictor = predictor;
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
        }
    }

    @Override
    public Double queryPrediction( Task task ) {
        synchronized ( linearPredictor ) {

            // If we have less than 5 tasks, predict using rules
            if ( n < firstTasks.length ) {
                for ( int i = 0; i < n; i++ ) {
                    if ( linearPredictor.getIndependentValue( firstTasks[i] ) > linearPredictor.getIndependentValue( task ) ) {
                        // Assuming that a larger input leads to a larger output
                        return max + fixedOffset;
                    }
                }
                return null;
            }

            if ( r == null ) {
                //Should not happen because we check for n before
                log.info( "Not enough data to predict" );
                return null;
            }

            //if positive linear relationship
            if ( r > 0.3 ) {
                final Double prediction = linearPredictor.queryPrediction( task );
                return checkPrediction( task, prediction );
            }

            // if no relationship
            if ( r >= 0 ) {
                log.info( "Using Max predictor: {}", Formater.formatBytes( (long) max ) );
                return max + fixedOffset; //TODO change to 95% percentile
            }

            // if negative linear relationship
            log.info( "Using Max predictor: {}", Formater.formatBytes( (long) max ) );
            return max + fixedOffset;
        }
    }

    @Nullable
    private Double checkPrediction( Task task, Double prediction ) {
        if ( prediction == null ) {
            return null;
        } else if ( prediction < min ) {
            log.info( "Using Min predictor: {}", Formater.formatBytes( (long) min ) );
            prediction = min;
        } else if ( prediction > max && getIndependentValue( task ) < maxX ) {
            log.info( "Using Max predictor: {}", Formater.formatBytes( (long) max ) );
            prediction = max;
        } else if ( getIndependentValue( task ) >= maxX && prediction < max ) {
            log.info( "Using Max predictor: {}", Formater.formatBytes( (long) max ) );
            prediction = max;
        }
        log.info( "Using linear predictor: {}", Formater.formatBytes( prediction.longValue() ) );
        final double offset = 2 * Math.sqrt( determineOffset() );
        return applyOffset( prediction, Math.max( fixedOffset, offset ) );
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

}
