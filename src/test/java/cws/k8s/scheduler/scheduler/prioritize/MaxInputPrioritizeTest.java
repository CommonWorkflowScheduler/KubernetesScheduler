package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MaxInputPrioritizeTest {

    @Test
    void sortTasks() {

        Task[] tasks = new Task[5];
        Task[] tasksCopy = new Task[5];
        for (int i = 0; i < 5; i++) {
            tasks[i] = new TestTask(5 - i);
            tasksCopy[i] = tasks[i];
        }

        List<Task> tasksList = new ArrayList<>( List.of(tasks) );
        new MaxInputPrioritize().sortTasks(tasksList);

        assertEquals(List.of(tasks), tasksList);
        assertEquals(List.of(tasksCopy), tasksList);


        Task t = tasks[0];
        tasks[0] = tasks[1];
        tasks[1] = t;

        assertNotEquals( List.of(tasks), tasksList );

        tasksList = new ArrayList<>( List.of(tasks) );
        new MaxInputPrioritize().sortTasks(tasksList);
        assertEquals(List.of(tasksCopy), tasksList);
        assertNotEquals( List.of(tasks), tasksList );

    }

}