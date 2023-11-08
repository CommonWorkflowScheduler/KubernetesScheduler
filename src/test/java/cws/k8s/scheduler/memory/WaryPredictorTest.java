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

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

/**
 * JUnit 5 Tests for the WaryPredictor
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
class WaryPredictorTest {

    /**
     * If there are < 3 observations, we cannot get a prediction
     */
    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2 })
    void testNoObservationsYet(int number) {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        log.info("param: {}", number);
        WaryPredictor waryPredictor = new WaryPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        
        for (int i=0; i<number; i++) {
            log.info("insert observation {}", i);
            // @formatter:off
            Observation observation = Observation.builder()
                    .task("taskName")
                    .taskName("taskName (1)")
                    .success(true)
                    .inputSize(123+i)
                    .ramRequest(BigDecimal.valueOf(0))
                    .peakVmem(BigDecimal.valueOf(1))
                    .peakRss(BigDecimal.valueOf(1))
                    .realtime(1000)
                    .build();
            // @formatter:on
            waryPredictor.addObservation(observation);
        }
        
        assertNull(waryPredictor.queryPrediction(task));
        
    }

    /**
     * If there are > 2 observations, we can get a prediction
     */
    @ParameterizedTest
    @ValueSource(ints = { 3, 4, 5 })
    void testSomeObservations(int number) {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        log.info("param: {}", number);
        WaryPredictor waryPredictor = new WaryPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        
        for (int i=0; i<number; i++) {
            log.info("insert observation {}", i);
            // @formatter:off
            Observation observation = Observation.builder()
                    .task("taskName")
                    .taskName("taskName (1)")
                    .success(true)
                    .inputSize(123+i)
                    .ramRequest(BigDecimal.valueOf(0))
                    .peakRss(BigDecimal.valueOf(1))
                    .peakVmem(BigDecimal.valueOf(1))
                    .realtime(1000)
                    .build();
            // @formatter:on
            waryPredictor.addObservation(observation);
        }
        
        assertNotNull(waryPredictor.queryPrediction(task));
        
    }

    /**
     * If there are > 3 observations, but with same inputSize, we cannot get a prediction
     */
    @ParameterizedTest
    @ValueSource(ints = { 3, 4, 5 })
    void testNoDifferentObservations(int number) {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        log.info("param: {}", number);
        WaryPredictor waryPredictor = new WaryPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        
        for (int i=0; i<number; i++) {
            log.info("insert observation {}", i);
            // @formatter:off
            Observation observation = Observation.builder()
                    .task("taskName")
                    .taskName("taskName (1)")
                    .success(true)
                    .inputSize(123)
                    .ramRequest(BigDecimal.valueOf(0))
                    .peakRss(BigDecimal.valueOf(1))
                    .peakVmem(BigDecimal.valueOf(1))
                    .realtime(1000)
                    .build();
            // @formatter:on
            waryPredictor.addObservation(observation);
        }
        
        assertNull(waryPredictor.queryPrediction(task));
        
    }

    /**
     * If there are > 2 errors, warePredictor will quit predicting
     */
    @Test
    void testAfter3Errors() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        WaryPredictor waryPredictor = new WaryPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1024l);
        
        long initialValue = 1000;
        
        for (int i=0; i<3; i++) {
            log.info("insert successful observation {}", i);
            // @formatter:off
            Observation observation = Observation.builder()
                    .task("taskName")
                    .taskName("taskName (1)")
                    .success(true)
                    .inputSize(123+i)
                    .ramRequest(BigDecimal.valueOf(initialValue))
                    .peakVmem(BigDecimal.valueOf(1))
                    .peakRss(BigDecimal.valueOf(1))
                    .realtime(1000)
                    .build();
            // @formatter:on
            waryPredictor.addObservation(observation);
        }
        
        assertNotNull(waryPredictor.queryPrediction(task));

        for (int i=0; i<3; i++) {
            log.info("insert failed observation {}", i);
            // @formatter:off
            Observation observation = Observation.builder()
                    .task("taskName")
                    .taskName("taskName (1)")
                    .success(false)
                    .inputSize(0)
                    .ramRequest(BigDecimal.valueOf(initialValue/2))
                    .peakRss(BigDecimal.valueOf(0))
                    .peakVmem(BigDecimal.valueOf(0))
                    .realtime(1000)
                    .build();
            // @formatter:on
            assertNotNull(waryPredictor.queryPrediction(task));
            waryPredictor.addObservation(observation);
        }
        assertEquals(initialValue, Long.parseLong( waryPredictor.queryPrediction(task) ));
    }
    
    
}
