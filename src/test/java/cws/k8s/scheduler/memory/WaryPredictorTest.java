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
import java.math.RoundingMode;

import org.apache.commons.math3.stat.regression.SimpleRegression;
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
     * If there are < 4 observations, we cannot get a prediction
     */
    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
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
     * If there are > 3 observations, we can get a prediction
     */
    @ParameterizedTest
    @ValueSource(ints = { 4, 5, 6 })
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
        
        for (int i=0; i<4; i++) {
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
    
    /**
     * Test a specific situation
     */
    //@Test
    void testSpecific() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());

        long is1 = 4652181;
        long v1 = 5082320896l;
        
        long is2 = 4647849;
        long v2 = 5082214400l;
        
        long is3 = 4589825;
        long v3 = 4948094976l;
        
        long is4 = 2464690;
        
        SimpleRegression sr = new SimpleRegression();
        sr.addData(is1, v1);
        sr.addData(is2, v2);
        sr.addData(is3, v3);
        long prediction = (long) sr.predict(is4);
        log.info("expected value = {}", prediction);
        
        WaryPredictor waryPredictor = new WaryPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", is4);

        // @formatter:off
        Observation o1 = Observation.builder().task("taskName").taskName("taskName (1)")
                .success(true)
                .inputSize(is1)
                .ramRequest(BigDecimal.valueOf(53687091200l))
                .peakVmem(BigDecimal.valueOf(v1))
                .peakRss(BigDecimal.valueOf(853852160l))
                .realtime(73000)
                .build();
        // @formatter:on
        waryPredictor.addObservation(o1);

        assertNull( waryPredictor.queryPrediction(task) );

        // @formatter:off
        Observation o2 = Observation.builder().task("taskName").taskName("taskName (1)")
                .success(true)
                .inputSize(is2)
                .ramRequest(BigDecimal.valueOf(53687091200l))
                .peakVmem(BigDecimal.valueOf(v2))
                .peakRss(BigDecimal.valueOf(858411008l))
                .realtime(71000)
                .build();
        // @formatter:on
        waryPredictor.addObservation(o2);

        assertNull( waryPredictor.queryPrediction(task) );

        // @formatter:off
        Observation o3 = Observation.builder().task("taskName").taskName("taskName (1)")
                .success(true)
                .inputSize(is3)
                .ramRequest(BigDecimal.valueOf(53687091200l))
                .peakVmem(BigDecimal.valueOf(v3))
                .peakRss(BigDecimal.valueOf(854892544l))
                .realtime(82000)
                .build();
        // @formatter:on
        waryPredictor.addObservation(o3);
        
        assertTrue(BigDecimal.valueOf(prediction).multiply(BigDecimal.valueOf(1.1)).setScale(0, RoundingMode.CEILING).compareTo(new BigDecimal( waryPredictor.queryPrediction(task) ) ) < 1 );

    }

    /**
     * Test ignore list
     */
    @Test
    void testIgnoreList() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());        
        WaryPredictor waryPredictor = new WaryPredictor();
        Task task = MemoryPredictorTest.createTask("taskName", 1000);
        Task task3 = MemoryPredictorTest.createTask("taskName", 1000, 3);

        long initialValue = 2000l;
        
        // @formatter:off
        Observation o2 = Observation.builder().task("taskName").taskName("taskName (2)")
                .success(false)
                .inputSize(1000)
                .ramRequest(BigDecimal.valueOf(initialValue))
                .peakVmem(BigDecimal.valueOf(853852160l))
                .peakRss(BigDecimal.valueOf(853852160l))
                .realtime(73000)
                .build();
        // @formatter:on
        waryPredictor.addObservation(o2);

        // @formatter:off
        Observation o1 = Observation.builder().task("taskName").taskName("taskName (1)")
                .success(false)
                .inputSize(1000)
                .ramRequest(BigDecimal.valueOf(53687091200l))
                .peakVmem(BigDecimal.valueOf(853852160l))
                .peakRss(BigDecimal.valueOf(853852160l))
                .realtime(73000)
                .build();
        // @formatter:on
        waryPredictor.addObservation(o1);

        log.info(waryPredictor.queryPrediction(task));
        assertEquals(initialValue, Long.parseLong(waryPredictor.queryPrediction(task)));
        log.info(waryPredictor.queryPrediction(task3));
        assertNull(waryPredictor.queryPrediction(task3));
        
        for (int i=0; i<4; i++) {
            log.info("insert successful observation {}", i);
            // @formatter:off
            Observation observation = Observation.builder()
                    .task("taskName")
                    .taskName("taskName ("+(4+i)+")")
                    .success(true)
                    .inputSize(123+i)
                    .ramRequest(BigDecimal.valueOf(123))
                    .peakVmem(BigDecimal.valueOf(1))
                    .peakRss(BigDecimal.valueOf(1))
                    .realtime(1000)
                    .build();
            // @formatter:on
            waryPredictor.addObservation(observation);
        }

        log.info(waryPredictor.queryPrediction(task3));
        assertNotNull(waryPredictor.queryPrediction(task3));

        log.info(waryPredictor.queryPrediction(task));
        assertEquals(initialValue, Long.parseLong(waryPredictor.queryPrediction(task)));

    }

    
}
