package cws.k8s.scheduler.prediction;

import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskMetrics;
import cws.k8s.scheduler.prediction.extractor.InputExtractor;
import cws.k8s.scheduler.prediction.extractor.MemoryExtractor;
import cws.k8s.scheduler.prediction.offset.MaxOffset;
import cws.k8s.scheduler.prediction.offset.PercentileOffset;
import cws.k8s.scheduler.prediction.offset.StandardDeviationOffset;
import cws.k8s.scheduler.prediction.offset.VarianceOffset;
import cws.k8s.scheduler.prediction.predictor.*;
import io.fabric8.kubernetes.api.builder.Builder;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

import static cws.k8s.scheduler.util.Formater.formatBytes;

@Slf4j
public class MemoryScaler extends TaskScaler {

    private final Long LOWEST_MEMORY_REQUEST;
    private final Long MAXIMUM_MEMORY_REQUEST;

    private final Builder<Predictor> predictorBuilder;

    /**
     * Create a new TaskScaler instance. The memory predictor to be used is
     * determined as follows:
     *
     * 1) use the value memoryPredictor provided in SchedulerConfig config
     *
     * 2) if (1) is set to "default", use the environment variable
     * MEMORY_PREDICTOR_DEFAULT
     *
     * @param config the SchedulerConfig for the execution
     */
    public MemoryScaler( SchedulerConfig config ) {
        String memoryPredictorString = config.memoryPredictor;
        if ("default".equalsIgnoreCase(memoryPredictorString)) {
            memoryPredictorString = System.getenv("MEMORY_PREDICTOR_DEFAULT");
        }
        Map<String,String> memoryPredictorParameter = null;
        String predictorString = memoryPredictorString;
        String parameterString = null;
        if ( memoryPredictorString.contains( "-" ) ) {
            predictorString = memoryPredictorString.substring( 0, memoryPredictorString.indexOf("-") );
            parameterString = memoryPredictorString.substring( memoryPredictorString.indexOf("-") + 1 );
        }
        memoryPredictorParameter = parsePredictorParams( parameterString );

        Builder<Predictor> cBuilder = applyPredictor( predictorString );

        cBuilder = applyOffset( cBuilder, memoryPredictorParameter.remove( "offset" ) );

        this.predictorBuilder = cBuilder;

        if ( !memoryPredictorParameter.isEmpty() ) {
            log.warn("unrecognized memoryPredictorParameter: " + memoryPredictorParameter );
        }

        MAXIMUM_MEMORY_REQUEST = config.maxMemory;
        LOWEST_MEMORY_REQUEST = config.minMemory;
        log.info( "MemoryScaler initialized with minMemory: {}, maxMemory: {}", formatBytes(LOWEST_MEMORY_REQUEST), formatBytes(MAXIMUM_MEMORY_REQUEST) );
    }

    private Builder<Predictor> applyOffset( final Builder<Predictor> builder, final String offsetValue ) {
        if ( offsetValue != null ) {
            if ( offsetValue.endsWith( "percentile" ) ) {
                final String substring = offsetValue.substring( 0, offsetValue.length() - "percentile".length() );
                int percentile = Integer.parseInt( substring );
                return () -> new PercentileOffset( builder.build(), percentile );
            } else if ( offsetValue.equals( "max" ) ) {
                return () -> new MaxOffset( builder.build() );
            } else if ( offsetValue.equals( "none" ) ) {
                return builder;
            } else if ( offsetValue.equals( "var" ) ) {
                return () -> new VarianceOffset( builder.build() );
            } else if ( offsetValue.endsWith( "std" ) ) {
                final String substring = offsetValue.substring( 0, offsetValue.length() - "std".length() );
                double factor = substring.isEmpty() ? 1 : Double.parseDouble( substring );
                return () -> new StandardDeviationOffset( factor, builder.build() );
            } else {
                throw new IllegalArgumentException("unrecognized offset parameter: " + offsetValue );
            }
        } else {
            return () -> new MaxOffset( builder.build() );
        }
    }

    private Builder<Predictor> applyPredictor( String predictorString ) {
        final InputExtractor inputExtractor = new InputExtractor();
        final MemoryExtractor outputExtractor = new MemoryExtractor();
        if ( predictorString.equalsIgnoreCase( "linear2" )) {
            log.debug( "using LinearPredictor2" );
            return () -> new LinearPredictorCustomLoss( inputExtractor, outputExtractor );
        } if ( predictorString.equalsIgnoreCase( "linear" )) {
            log.debug( "using LinearPredictor" );
            return () -> new LinearPredictorSquaredLoss( inputExtractor, outputExtractor );
        } else if ( predictorString.equalsIgnoreCase( "mean" )) {
            log.debug( "using MeanPredictor" );
            return () -> new MeanPredictor( outputExtractor );
        } else if ( predictorString.equalsIgnoreCase( "ponder2special" )) {
            log.debug( "using PonderingPredictor" );
            return () -> new PonderingPredictorSpecial( new LinearPredictorCustomLoss( inputExtractor, outputExtractor ) );
        } else if ( predictorString.equalsIgnoreCase( "ponderSpecial" )) {
            log.debug( "using PonderingPredictor" );
            return () -> new PonderingPredictorSpecial( new LinearPredictorSquaredLoss( inputExtractor, outputExtractor ) );
        } else if ( predictorString.equalsIgnoreCase( "ponder2" )) {
            log.debug( "using PonderingPredictor" );
            return () -> new PonderingPredictor( new LinearPredictorCustomLoss( inputExtractor, outputExtractor ) );
        } else if ( predictorString.equalsIgnoreCase( "ponder" )) {
            log.debug( "using PonderingPredictor" );
            return () -> new PonderingPredictor( new LinearPredictorSquaredLoss( inputExtractor, outputExtractor ) );
        } else if ( predictorString.toLowerCase().startsWith( "const" ) ) {
            final String substring = predictorString.substring( "const".length() );
            final long value = substring.length() == 0 ? 0 : Long.parseLong( substring );
            log.debug( "using ConstantPredictor with value: {}", value );
            return () -> new ConstantNumberPredictor( outputExtractor, value );
        } else if ( predictorString.toLowerCase().startsWith( "poly" ) ) {
            final String substring = predictorString.substring( "poly".length() );
            final int value = substring.length() == 0 ? 0 : Integer.parseInt( substring );
            log.debug( "using PolyPredictor with value: {}", value );
            return () -> new PolynomialPredictor( inputExtractor, outputExtractor, value );
        } else {
            throw new IllegalArgumentException("unrecognized memoryPredictorString: " + predictorString);
        }
    }

    @Override
    protected boolean isValid( Task task ) {
        final TaskMetrics taskMetrics = task.getTaskMetrics();
        return taskMetrics != null
                // for short running tasks this can not always be measured
                && taskMetrics.getPeakRss() > 0
                // runtime should not be negative
                && taskMetrics.getRealtime() >= 0;
    }

    @Override
    protected boolean applyToThisTask( Task task ) {
        if ( task.getConfig().getRepetition() > 0 ) {
            log.debug( "task {} is a repetition, not changing it", task.getConfig().getName() );
            return false;
        }
        BigDecimal taskRequest = task.getOriginalMemoryRequest();
        if (taskRequest.compareTo(BigDecimal.ZERO) == 0) {
            log.debug( "task {} had no prior requirements", task.getConfig().getName() );
            return false;
        }
        return true;
    }

    protected void scaleTask( Task task, Double prediction, long predictorVersion ) {
        if ( prediction != null ) {
            long newRequestValue = prediction.longValue();
            log.debug("predictor proposes {} for task {}", prediction, task.getConfig().getName());

            // if our prediction is a very low value, the pod might not start. Make sure it has at least 256MiB
            if ( LOWEST_MEMORY_REQUEST != null && newRequestValue < LOWEST_MEMORY_REQUEST ) {
                log.debug("Prediction of {} is lower than {}. Automatically increased.", formatBytes( newRequestValue), formatBytes( LOWEST_MEMORY_REQUEST));
                newRequestValue = LOWEST_MEMORY_REQUEST;
            } else if ( MAXIMUM_MEMORY_REQUEST != null && newRequestValue > MAXIMUM_MEMORY_REQUEST ) {
                log.debug("Prediction of {} is higher than {}. Automatically decreased.", formatBytes( newRequestValue), formatBytes( MAXIMUM_MEMORY_REQUEST));
                newRequestValue = MAXIMUM_MEMORY_REQUEST;
            } else if ( newRequestValue > task.getOriginalMemoryRequest().longValue() ) {
                log.debug("Prediction of {} is higher than original request {}. Automatically decreased.", formatBytes( newRequestValue), formatBytes( task.getOriginalMemoryRequest().longValue()));
                newRequestValue = task.getOriginalMemoryRequest().longValue();
            }
            long newValue = roundUpToFullMB( newRequestValue );
            log.info("resizing {} to {} bytes", task.getConfig().getName(), formatBytes(newValue));
            task.setPlannedMemoryInBytes( newValue, predictorVersion );
        }
    }

    static long roundUpToFullMB( long bytes ) {
        final long MB = 1024L * 1024L;
        final long l = bytes % MB;
        return l == 0 ? bytes : bytes + MB - l;
    }

    @Override
    protected Predictor createPredictor( String taskName ) {
        return predictorBuilder.build();
    }

    @Override
    protected long getTaskVersionForPredictor( Task task ) {
        return task.getMemoryPredictionVersion();
    }

}
