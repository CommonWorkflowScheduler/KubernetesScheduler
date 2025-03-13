package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RankMaxPrioritizeTest {

    @Test
    void sortTasks() {
        Task a = new TestTask( 4, 1, 1 );
        Task b = new TestTask( 4, 0, 2 );
        Task c = new TestTask( 4, 1, 3 );
        Task d = new TestTask( 4, 0, 4 );

        final List<Task> tasks = new ArrayList<>(List.of( a, b, c, d ));
        new RankMaxPrioritize().sortTasks( tasks );
        assertEquals( List.of( c, a, d, b ), tasks);
    }
}