package cws.k8s.scheduler.prediction;

import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.extractor.InputExtractor;
import cws.k8s.scheduler.prediction.extractor.MemoryExtractor;
import cws.k8s.scheduler.prediction.offset.MaxOffset;
import cws.k8s.scheduler.prediction.predictor.LinearPredictor;
import io.fabric8.kubernetes.api.builder.Builder;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

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
        String memoryPredictorParameter = null;
        if ( memoryPredictorString.contains( "-" ) ) {
            memoryPredictorString = memoryPredictorString.substring( 0, memoryPredictorString.indexOf("-") );
            memoryPredictorParameter = memoryPredictorString.substring( memoryPredictorString.indexOf("-") + 1 );
        }
        final InputExtractor inputExtractor = new InputExtractor();
        final MemoryExtractor outputExtractor = new MemoryExtractor();

        switch (memoryPredictorString.toLowerCase()) {
            case "linear":
                log.debug("using LinearPredictor");
                this.predictorBuilder = () -> new MaxOffset( new LinearPredictor( inputExtractor, outputExtractor ) );
                break;

            default:
                throw new IllegalArgumentException("unrecognized memoryPredictorString: " + memoryPredictorString);
        }
        MAXIMUM_MEMORY_REQUEST = config.maxMemory;
        LOWEST_MEMORY_REQUEST = config.minMemory;
        log.info( "MemoryScaler initialized with minMemory: {}, maxMemory: {}", formatBytes(LOWEST_MEMORY_REQUEST), formatBytes(MAXIMUM_MEMORY_REQUEST) );
    }

    @Override
    protected boolean applyToThisTask( Task task ) {
        if ( task.getConfig().getRepetition() > 0 ) {
            log.debug( "task {} is a repetition, not changing it", task.getConfig().getName() );
            return false;
        }
        BigDecimal taskRequest = task.getMemoryRequest();
        if (taskRequest.compareTo(BigDecimal.ZERO) == 0) {
            log.debug( "task {} had no prior requirements", task.getConfig().getName() );
            return false;
        }
        return true;
    }

    protected void scaleTask( Task task ) {
        log.debug("1 unscheduledTask: {} {} {}", task.getConfig().getTask(), task.getConfig().getName(),
                formatBytes(task.getMemoryRequest().longValue()));

        Double newRequestValue = null;

        final Predictor predictor = predictors.get( task.getConfig().getTask() );

        if ( predictor == null ) {
            return;
        }

        // query suggestion
        Double prediction = predictor.queryPrediction(task);

        // sanity check for our prediction
        if ( prediction != null ) {
            // we have a prediction and it fits into the cluster
            newRequestValue = prediction;
            log.debug("predictor proposes {} for task {}", prediction, task.getConfig().getName());

            // if our prediction is a very low value, the pod might not start. Make sure it has at least 256MiB
            if ( LOWEST_MEMORY_REQUEST != null && newRequestValue < LOWEST_MEMORY_REQUEST ) {
                log.debug("Prediction of {} is lower than {}. Automatically increased.", newRequestValue, LOWEST_MEMORY_REQUEST);
                newRequestValue = (double) LOWEST_MEMORY_REQUEST;
            } else if ( MAXIMUM_MEMORY_REQUEST != null && newRequestValue > MAXIMUM_MEMORY_REQUEST ) {
                log.debug("Prediction of {} is higher than {}. Automatically decreased.", newRequestValue, MAXIMUM_MEMORY_REQUEST);
                newRequestValue = (double) MAXIMUM_MEMORY_REQUEST;
            }

        }

        if (newRequestValue != null) {
            log.info("resizing {} to {} bytes", task.getConfig().getName(), formatBytes(newRequestValue.longValue()));
            task.setPlannedMemoryInBytes( newRequestValue.longValue() );
        }
    }

    @Override
    protected Predictor createPredictor( String taskName ) {
        return predictorBuilder.build();
    }

}
