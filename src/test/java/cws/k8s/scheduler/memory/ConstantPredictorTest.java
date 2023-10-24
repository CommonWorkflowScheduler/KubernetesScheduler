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
 * JUnit 4 Tests for the ConstantPredictor
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
public class ConstantPredictorTest {

    /**
     * If there are no observations, we cannot get a prediction
     * 
     */
    @Test
    public void testNoObservationsYet() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        ConstantPredictor constantPredictor = new ConstantPredictor();
        Task task = MemoryPredictorTest.createTask("taskName");
        assertNull(constantPredictor.querySuggestion(task));
    }

    /**
     * If there is one observation, we get a prediction
     * 
     */
    @Test
    public void testOneObservation() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        ConstantPredictor constantPredictor = new ConstantPredictor();
        Task task = MemoryPredictorTest.createTask("taskName");
        // @formatter:off
        Observation observation = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(BigDecimal.valueOf(0))
                .ramLimit(BigDecimal.valueOf(0))
                .peakRss(BigDecimal.valueOf(0))
                .build();
        // @formatter:on
        constantPredictor.addObservation(observation);
        assertNotNull(constantPredictor.querySuggestion(task));
    }
    
    /**
     * If there are two observations, we will also get a suggestion
     */
    @Test
    public void testTwoObservations() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        ConstantPredictor constantPredictor = new ConstantPredictor();
        Task task = MemoryPredictorTest.createTask("taskName");
        // @formatter:off
        Observation observation1 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(BigDecimal.valueOf(0))
                .ramLimit(BigDecimal.valueOf(0))
                .peakRss(BigDecimal.valueOf(0))
                .build();
        Observation observation2 = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(BigDecimal.valueOf(0))
                .ramLimit(BigDecimal.valueOf(0))
                .peakRss(BigDecimal.valueOf(0))
                .build();
        // @formatter:on
        constantPredictor.addObservation(observation1);
        constantPredictor.addObservation(observation2);
        assertNotNull(constantPredictor.querySuggestion(task));
    }
    
    /**
     * The prediction decreases right after one observation
     * 
     */
    @Test
    public void testDecreasePredictionAfterOneObservation() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        ConstantPredictor constantPredictor = new ConstantPredictor();
        Task task = MemoryPredictorTest.createTask("taskName");
        
        BigDecimal reserved = BigDecimal.valueOf(4l*1024*1024*1024);
        BigDecimal used = BigDecimal.valueOf(2l*1024*1024*1024);
        
        // @formatter:off
        Observation observation = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(reserved)
                .ramLimit(reserved)
                .peakRss(used)
                .build();
        // @formatter:on
        constantPredictor.addObservation(observation);
        String suggestionStr = constantPredictor.querySuggestion(task);
        log.debug("suggestion is: {}", suggestionStr);
        // 1. There is a suggestion at all
        assertNotNull(suggestionStr);
        BigDecimal suggestion = new BigDecimal(suggestionStr);
        // 2. The suggestion is lower than the reserved value was
        assertTrue(suggestion.compareTo(reserved) < 0);
        // 3. The suggestion is higher than the used value was
        assertTrue(suggestion.compareTo(used) > 0);
    }

    /**
     * The prediction decreases further, if another successful observation is 
     * made
     * 
     */
    @Test
    public void testDecreasePredictionAfterTwoObservation() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        ConstantPredictor constantPredictor = new ConstantPredictor();
        Task task = MemoryPredictorTest.createTask("taskName");
        
        BigDecimal reserved = BigDecimal.valueOf(4l*1024*1024*1024);
        BigDecimal used = BigDecimal.valueOf(2l*1024*1024*1024);
        
        // @formatter:off
        Observation observation = Observation.builder()
                .task("taskName")
                .taskName("taskName (1)")
                .success(true)
                .inputSize(0)
                .ramRequest(reserved)
                .ramLimit(reserved)
                .peakRss(used)
                .build();
        // @formatter:on
        constantPredictor.addObservation(observation);
        String suggestionStr = constantPredictor.querySuggestion(task);
        log.debug("suggestion is: {}", suggestionStr);
        // 1. There is a suggestion at all
        assertNotNull(suggestionStr);
        BigDecimal suggestion = new BigDecimal(suggestionStr);
        // 2. The suggestion is lower than the reserved value was
        assertTrue(suggestion.compareTo(reserved) < 0);
        // 3. The suggestion is higher than the used value was
        assertTrue(suggestion.compareTo(used) > 0);
    }

}
