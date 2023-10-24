/*
 * Copyright (c) 2023, Florian Friederici. All rights reserved.
 * 
 * This code is free software: you can redistribute it and/or modify it under 
 * the terms of the GNU General Public License as published by the Free 
 * Software Foundation, either version 3 of the License, or (at your option) 
 * any later version.
 * 
 * This code is distributed in the hope that it will be useful, but WITHOUT ANY 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more 
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with 
 * this work. If not, see <https://www.gnu.org/licenses/>. 
 */

package cws.k8s.scheduler.memory;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Common methods for all MemoryPredictor Tests
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
public class MemoryPredictorTest {

    /**
     * Helper that creates tasks for the tests
     * 
     * Note: There a two fields that contain the name of the task within the
     * taskConfig. The first one, taskConfig.task, contains the process name from
     * Nextflow. The second one, taskConfig.name, has a number added.
     * 
     * @return the newly created Task
     */
    static Task createTask(String name) {
        TaskConfig taskConfig = new TaskConfig(name);
        ReflectionTestUtils.setField(taskConfig, "name", name + " (1)");
        DAG dag = new DAG();
        List<Vertex> processes = Arrays.asList(new Process(name, 0));
        dag.registerVertices(processes);
        Task task = new Task(taskConfig, dag);
        return task;
    }

    /**
     * Execute observationSanityCheck on all predictors
     * 
     */
    @Test
    public void testSanityChecksOnAllPredictors() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());

        NonePredictor nonePredictor = new NonePredictor();
        observationSanityCheck(nonePredictor);

        ConstantPredictor constantPredictor = new ConstantPredictor();
        observationSanityCheck(constantPredictor);

        LinearPredictor linearPredictor = new LinearPredictor();
        observationSanityCheck(linearPredictor);
    }
    
    /**
     * A runtime exception is thrown, when the observation values look suspicious.
     * No suggestion will be available then.
     */
    void observationSanityCheck(MemoryPredictor memoryPredictor) {
        Task task = MemoryPredictorTest.createTask("taskName");
        // @formatter:off
        Observation observation1 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(-1)
                .ramRequest(BigDecimal.valueOf(0))
                .ramLimit(BigDecimal.valueOf(0))
                .peakRss(BigDecimal.valueOf(0))
                .build();
        Observation observation2 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(BigDecimal.valueOf(100l))
                .ramLimit(BigDecimal.valueOf(99l))
                .peakRss(BigDecimal.valueOf(0))
                .build();
        // @formatter:on

        Exception e1 = assertThrows(ObservationException.class, () -> memoryPredictor.addObservation(observation1));
        log.info("exception was: {}", e1.getMessage());

        Exception e2 = assertThrows(ObservationException.class, () -> memoryPredictor.addObservation(observation2));
        log.info("exception was: {}", e2.getMessage());
        
        assertNull(memoryPredictor.querySuggestion(task));
    }

}
