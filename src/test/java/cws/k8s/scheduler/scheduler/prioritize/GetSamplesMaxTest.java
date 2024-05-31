package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GetSamplesMaxMaxTest {

    @Test
    void sortTasksNoTie() {

        Task a = new TestTask( 1, 0 );
        Task b = new TestTask( 5, 0 );
        Task c = new TestTask( 3, 0 );
        Task d = new TestTask( 2, 0 );
        Task e = new TestTask( 4, 0 );

        final List<Task> tasks = new ArrayList<>(List.of( a, b, c, d, e ));
        new GetSamplesMaxPrioritize().sortTasks( tasks );
        assertEquals( List.of( a, d, c, e, b ), tasks);

    }

    @Test
    void sortTasksTie1() {

        Task a = new TestTask( 1, 0 );
        Task b = new TestTask( 5, 0 );
        Task c = new TestTask( 3, 0 );
        Task d = new TestTask( 4, 0 );
        Task e = new TestTask( 4, 1 );

        final List<Task> tasks = new ArrayList<>(List.of( a, b, c, d, e ));
        new GetSamplesMaxPrioritize().sortTasks( tasks );
        assertEquals( List.of( a, c, e, d, b ), tasks);

    }

    @Test
    void sortTasksTie2() {

        Task a = new TestTask( 1, 0 );
        Task b = new TestTask( 5, 0 );
        Task c = new TestTask( 3, 0 );
        Task d = new TestTask( 4, 1 );
        Task e = new TestTask( 4, 0 );

        final List<Task> tasks = new ArrayList<>(List.of( a, b, c, d, e ));
        new GetSamplesMaxPrioritize().sortTasks( tasks );
        assertEquals( List.of( a, c, d, e, b ), tasks);

    }

    @Test
    void sortTasksTieTie() {

        Task a = new TestTask( 4, 1, 1 );
        Task b = new TestTask( 4, 0, 2 );
        Task c = new TestTask( 4, 1, 3 );
        Task d = new TestTask( 4, 0, 4 );

        final List<Task> tasks = new ArrayList<>(List.of( a, b, c, d ));
        new GetSamplesMaxPrioritize().sortTasks( tasks );
        assertEquals( List.of( c, a, d, b ), tasks);

    }

    @Test
    void sortTasksTieTieMax() {

        Task a = new TestTask( 4, 1, 1 );
        Task b = new TestTask( 5, 0, 2 );
        Task c = new TestTask( 5, 1, 3 );
        Task d = new TestTask( 5, 0, 4 );

        final List<Task> tasks = new ArrayList<>(List.of( a, b, c, d ));
        new GetSamplesMaxPrioritize().sortTasks( tasks );
        assertEquals( List.of( a, c, d, b ), tasks);

    }

    @Test
    void sortTasksTieTieMax2() {

        Task a = new TestTask( 4, 1, 1 );
        Task b = new TestTask( 5, 0, 2 );
        Task c = new TestTask( 6, 1, 3 );
        Task d = new TestTask( 7, 0, 4 );

        final List<Task> tasks = new ArrayList<>(List.of( a, b, c, d ));
        new GetSamplesMaxPrioritize().sortTasks( tasks );
        assertEquals( List.of( a, c, d, b ), tasks);

    }

}