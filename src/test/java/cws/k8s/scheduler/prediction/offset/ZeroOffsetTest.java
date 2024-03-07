package cws.k8s.scheduler.prediction.offset;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.predictor.ConstantNumberPredictor;
import cws.k8s.scheduler.prediction.predictor.TestTask;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ZeroOffsetTest {

    @Test
    void getOffset() {

        final ZeroOffset zeroOffset = new ZeroOffset( new ConstantNumberPredictor( t -> ((TestTask) t).y , 0 ));
        Task[] tasks = {
                new TestTask( 1d,1d ),
                new TestTask( 1d,2d ),
                new TestTask( 1d,3d ),
                new TestTask( 1d,1d )
        };

        assertEquals( 0, zeroOffset.getOffset( List.of( tasks ) ) );

    }
}