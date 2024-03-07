package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.predictor.ConstantNumberPredictor;
import cws.k8s.scheduler.prediction.predictor.TestTask;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PercentileOffsetTest {

    private void shuffleArray(Task[] ar)
    {
        for (int i = ar.length - 1; i > 0; i--)
        {
            int index = (int)(Math.random() * (i + 1));
            Task a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

    @Test
    void getOffset50Percentile4Values() {

        final PercentileOffset percentileOffset = new PercentileOffset( new ConstantNumberPredictor( t -> ((TestTask) t).y , 0 ), 50);
        Task[] tasks = {
                new TestTask( 1d,1d ),
                new TestTask( 1d,2d ),
                new TestTask( 1d,4d ),
                new TestTask( 1d,3d ),
        };

        for ( int i = 0; i < 100; i++ ) {
            shuffleArray( tasks );
            assertEquals( 2.5, percentileOffset.getOffset( List.of( tasks ) ) );
        }

    }

    @Test
    void getOffset75Percentile4Values() {

        final PercentileOffset percentileOffset = new PercentileOffset( new ConstantNumberPredictor( t -> ((TestTask) t).y , 0 ), 75);
        Task[] tasks = {
                new TestTask( 1d,1d ),
                new TestTask( 1d,2d ),
                new TestTask( 1d,4d ),
                new TestTask( 1d,3d ),
        };

        for ( int i = 0; i < 100; i++ ) {
            shuffleArray( tasks );
            assertEquals( 3.75, percentileOffset.getOffset( List.of( tasks ) ) );
        }

    }

    @Test
    void getOffset50Percentile5Values() {

        final PercentileOffset percentileOffset = new PercentileOffset( new ConstantNumberPredictor( t -> ((TestTask) t).y , 0 ), 50);
        Task[] tasks = {
                new TestTask( 1d,1d ),
                new TestTask( 1d,2d ),
                new TestTask( 1d,4d ),
                new TestTask( 1d,2d ),
                new TestTask( 1d,3d ),
        };

        for ( int i = 0; i < 100; i++ ) {
            shuffleArray( tasks );
            assertEquals( 2, percentileOffset.getOffset( List.of( tasks ) ) );
        }

    }

}