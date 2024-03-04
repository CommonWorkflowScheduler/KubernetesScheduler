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
 * JUnit 4 Tests for the NonePredictor
 * 
 * @author Florian Friederici
 */
@Slf4j
public class NonePredictorTest {

    /**
     * NonePredictor shall never give a prediction. Test 1: when no observations
     * were inserted
     */
    @Test
    public void testNoObservationsYet() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        NonePredictor nonePredictor = new NonePredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        assertNull(nonePredictor.queryPrediction(task));
    }

    /**
     * NonePredictor shall never give a prediction. Test 2: when one observation was
     * inserted
     */
    @Test
    public void testOneObservation() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        NonePredictor nonePredictor = new NonePredictor();
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
        nonePredictor.addObservation(observation);
        assertNull(nonePredictor.queryPrediction(task));
    }

    /**
     * NonePredictor shall never give a prediction. Test 3: when two observations
     * were inserted
     */
    @Test
    public void testTwoObservations() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        NonePredictor nonePredictor = new NonePredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        // @formatter:off
        Observation observation1 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(BigDecimal.valueOf(0))
                .peakRss(BigDecimal.valueOf(1))
                .realtime(1000)
                .build();
        Observation observation2 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(BigDecimal.valueOf(0))
                .peakRss(BigDecimal.valueOf(1))
                .realtime(1000)
                .build();
        // @formatter:on
        nonePredictor.addObservation(observation1);
        nonePredictor.addObservation(observation2);
        assertNull(nonePredictor.queryPrediction(task));
    }

}
