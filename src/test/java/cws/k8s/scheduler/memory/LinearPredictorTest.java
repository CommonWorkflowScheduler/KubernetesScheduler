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

import static org.junit.Assert.*;

import java.math.BigDecimal;

import org.junit.Test;

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

/**
 * JUnit 4 Tests for the LinearPredictor
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
public class LinearPredictorTest {

    /**
     * If there are no observations, we cannot get a prediction
     */
    @Test
    public void testNoObservationsYet() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        LinearPredictor linearPredictor = new LinearPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        assertNull(linearPredictor.queryPrediction(task));
    }

    /**
     * If there is only one observation, we cannot get a prediction either
     */
    @Test
    public void testOneObservation() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        LinearPredictor linearPredictor = new LinearPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        // @formatter:off
        Observation observation = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(BigDecimal.valueOf(0))
                .peakRss(BigDecimal.valueOf(1))
                .realtime(1000)
                .build();
        // @formatter:on
        linearPredictor.addObservation(observation);
        assertNull(linearPredictor.queryPrediction(task));
    }

    /**
     * If there are two observations, we can get a first prediction
     */
    @Test
    public void testTwoObservations() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        LinearPredictor linearPredictor = new LinearPredictor();
        // @formatter:off
        Observation observation1 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(1024l)
                .ramRequest(BigDecimal.valueOf(4l*1024*1024*1024))
                .peakRss(BigDecimal.valueOf(1l*1024*1024*1024))
                .realtime(1000)
                .build();
        Observation observation2 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(2048)
                .ramRequest(BigDecimal.valueOf(4l*1024*1024*1024))
                .peakRss(BigDecimal.valueOf(2l*1024*1024*1024))
                .realtime(1000)
                .build();
        // @formatter:on
        linearPredictor.addObservation(observation1);
        linearPredictor.addObservation(observation2);

        Task task1 = MemoryPredictorTest.createTask("taskName", 512l);
        BigDecimal suggestion1 = linearPredictor.queryPrediction(task1);
        assertNotNull(suggestion1);
        log.info("suggestion 1 is: {}", suggestion1);

        Task task2 = MemoryPredictorTest.createTask("taskName", 1024l);
        BigDecimal suggestion2 = linearPredictor.queryPrediction(task2);
        assertNotNull(suggestion2);
        log.info("suggestion 2 is: {}", suggestion2);
        assertTrue(suggestion2.compareTo(suggestion1) > 0);

        Task task3 = MemoryPredictorTest.createTask("taskName", 1536l);
        BigDecimal suggestion3 = linearPredictor.queryPrediction(task3);
        assertNotNull(suggestion3);
        log.info("suggestion 3 is: {}", suggestion3);
        assertTrue(suggestion3.compareTo(suggestion2) > 0);

        Task task4 = MemoryPredictorTest.createTask("taskName", 2048l);
        BigDecimal suggestion4 = linearPredictor.queryPrediction(task4);
        assertNotNull(suggestion4);
        log.info("suggestion 4 is: {}", suggestion4);
        assertTrue(suggestion4.compareTo(suggestion3) > 0);

        Task task5 = MemoryPredictorTest.createTask("taskName", 4096l);
        BigDecimal suggestion5 = linearPredictor.queryPrediction(task5);
        assertNotNull(suggestion5);
        log.info("suggestion 5 is: {}", suggestion5);
        assertTrue(suggestion5.compareTo(suggestion4) > 0);
    }

    /**
     * Test that predictions cannot get negative
     */
    @Test
    public void testNoNegativePredicitons() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        LinearPredictor linearPredictor = new LinearPredictor();
        // @formatter:off
        Observation observation1 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(3)
                .ramRequest(BigDecimal.valueOf(3))
                .peakRss(BigDecimal.valueOf(3))
                .realtime(1000)
                .build();
        Observation observation2 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(2)
                .ramRequest(BigDecimal.valueOf(1))
                .peakRss(BigDecimal.valueOf(1))
                .realtime(1000)
                .build();
        // @formatter:on
        linearPredictor.addObservation(observation1);
        linearPredictor.addObservation(observation2);

        Task task1 = MemoryPredictorTest.createTask("taskName", 1);
        BigDecimal suggestion1 = linearPredictor.queryPrediction(task1);
        assertNull(suggestion1);
    }

    /**
     * test for observation with success = false
     */
    @Test
    public void testObservationFailed() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        LinearPredictor linearPredictor = new LinearPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        // @formatter:off
        Observation observation = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(false)
                .inputSize(0)
                .ramRequest(BigDecimal.valueOf(0))
                .peakRss(BigDecimal.valueOf(1))
                .realtime(1000)
                .build();
        // @formatter:on
        linearPredictor.addObservation(observation);
        assertNull(linearPredictor.queryPrediction(task));
        linearPredictor.addObservation(observation);
        assertNull(linearPredictor.queryPrediction(task));
        linearPredictor.addObservation(observation);
        assertNull(linearPredictor.queryPrediction(task));
    }

    
}
