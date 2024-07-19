package cws.k8s.scheduler.prediction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.TaskMetrics;
import cws.k8s.scheduler.prediction.predictor.TestTask;
import org.junit.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MemoryScalerTest {

    long MB = 1024 * 1024;

    private SchedulerConfig getSchedulerConfig( long minMemory, long maxMemory, String memoryPredictor ) {
        ObjectMapper objectMapper = new ObjectMapper();
        SchedulerConfig schedulerConfig = null;
        try {
            final String json = "{"
                    + "\"maxMemory\":" + maxMemory
                    + ",\"minMemory\":" + minMemory
                    + ",\"memoryPredictor\":\"" + memoryPredictor
                    + "\"}";
            schedulerConfig = objectMapper.readValue( json, SchedulerConfig.class);
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
        return schedulerConfig;
    }

    private TaskMetrics getTaskMetric( long runtime, long peakRss ) {
        ObjectMapper objectMapper = new ObjectMapper();
        TaskMetrics taskMetrics = null;
        try {
            final String json = "{"
                    + "\"realtime\":" + runtime
                    + ",\"peakRss\":" + peakRss
                    + "}";
            taskMetrics = objectMapper.readValue( json, TaskMetrics.class);
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
        return taskMetrics;
    }

    @Test
    public void initialTest() {
        assertThrowsExactly( IllegalArgumentException.class, () -> new MemoryScaler( getSchedulerConfig( 1024, 2048, "not there" ) ) );
        assertDoesNotThrow( () -> new MemoryScaler( getSchedulerConfig( 1024, 2048, "linear" ) ) );
    }

    @Test
    public void predictAfterSingleTask() {

        final MemoryScaler memoryScaler = new MemoryScaler( getSchedulerConfig( 1024 * MB, 2048 * MB, "linear" ) );
        final TestTask task = new TestTask( 1, 1 );
        task.setTaskMetrics( getTaskMetric( 1, MB ) );
        memoryScaler.afterTaskFinished( task );
        final TestTask task2 = new TestTask( 2, 2 );
        assertEquals( 2 * MB, task2.getNewMemoryRequest() );
        memoryScaler.beforeTasksScheduled( List.of(task2) );
        assertEquals( 2 * MB, task2.getNewMemoryRequest() );

    }

    @Test
    public void predictAfterTwoTasks() {

        final MemoryScaler memoryScaler = new MemoryScaler( getSchedulerConfig( 0, 2048*MB, "linear" ) );
        final TestTask task = new TestTask( 4, 1 );
        task.setTaskMetrics( getTaskMetric( 1, 1 * MB ) );
        memoryScaler.afterTaskFinished( task );

        final TestTask task2 = new TestTask( 4, 2 );
        task2.setTaskMetrics( getTaskMetric( 2, 2 * MB ) );
        memoryScaler.afterTaskFinished( task2 );

        final TestTask taskToPredict = new TestTask( 4, 3 );
        assertEquals( 4 * MB, taskToPredict.getNewMemoryRequest() );
        memoryScaler.beforeTasksScheduled( List.of(taskToPredict) );
        assertEquals( 3 * MB, taskToPredict.getNewMemoryRequest() );

    }

    @Test
    public void considerMin() {

        final MemoryScaler memoryScaler = new MemoryScaler( getSchedulerConfig( 1024 * MB, 2048 * MB, "linear" ) );
        final TestTask task = new TestTask( 4, 1 );
        task.setTaskMetrics( getTaskMetric( 1, MB ) );
        memoryScaler.afterTaskFinished( task );

        final TestTask task2 = new TestTask( 4, 2 );
        task2.setTaskMetrics( getTaskMetric( 2, 2 * MB ) );
        memoryScaler.afterTaskFinished( task2 );

        final TestTask taskToPredict = new TestTask( 4, 3 );
        assertEquals( 4 * MB, taskToPredict.getNewMemoryRequest() );
        memoryScaler.beforeTasksScheduled( List.of(taskToPredict) );
        assertEquals( 1024 * MB, taskToPredict.getNewMemoryRequest() );

    }

    @Test
    public void considerMax() {

        final MemoryScaler memoryScaler = new MemoryScaler( getSchedulerConfig( 0, 2 * MB, "linear" ) );
        final TestTask task = new TestTask( 4, 1 );
        task.setTaskMetrics( getTaskMetric( 1, MB ) );
        memoryScaler.afterTaskFinished( task );

        final TestTask task2 = new TestTask( 4, 2 );
        task2.setTaskMetrics( getTaskMetric( 2, 2 * MB ) );
        memoryScaler.afterTaskFinished( task2 );

        final TestTask taskToPredict = new TestTask( 4, 3 );
        assertEquals( 4 * MB, taskToPredict.getNewMemoryRequest() );
        memoryScaler.beforeTasksScheduled( List.of(taskToPredict) );
        assertEquals( 2 * MB, taskToPredict.getNewMemoryRequest() );

    }

    @Test
    public void predictWithoutTask() {

        final MemoryScaler memoryScaler = new MemoryScaler( getSchedulerConfig( 1024 * MB, 2048 * MB, "linear" ) );
        final TestTask task2 = new TestTask( 2, 2 );
        assertEquals( 2 * MB, task2.getNewMemoryRequest() );
        memoryScaler.beforeTasksScheduled( List.of(task2) );
        assertEquals( 2 * MB, task2.getNewMemoryRequest() );

    }

    @Test
    public void roundUpToFullMB() {
        long mb = 1024 * 1024;
        assertEquals( 0, MemoryScaler.roundUpToFullMB( 0 ) );
        assertEquals( mb, MemoryScaler.roundUpToFullMB( 1024 ) );
        assertEquals( mb, MemoryScaler.roundUpToFullMB( 1024 + 1 ) );
        assertEquals( mb, MemoryScaler.roundUpToFullMB( mb ) );
        assertEquals( mb * 2, MemoryScaler.roundUpToFullMB( mb + 1 ) );
        assertEquals( mb * 2, MemoryScaler.roundUpToFullMB( 2 * mb ) );
        assertEquals( mb * 3, MemoryScaler.roundUpToFullMB( 2 * mb + 1 ) );


    }
}