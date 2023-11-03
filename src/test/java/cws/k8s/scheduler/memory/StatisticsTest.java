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
import org.mockito.Mockito;

import cws.k8s.scheduler.scheduler.Scheduler;
import lombok.extern.slf4j.Slf4j;

/**
 * JUnit 4 Tests for the Statistics
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
public class StatisticsTest {

    private Statistics mockStatistics() {
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        MemoryPredictor memoryPredictor = new NonePredictor();
        Statistics statistics = new Statistics(scheduler, memoryPredictor);
        return statistics;
    }
    
    /**
     * If no observations are inserted, csv must be empty
     */
    @Test
    public void testNoObservations() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());

        Statistics statistics = mockStatistics();
        
        String csv = statistics.exportCsv(0);
        log.info(csv);
        assertEquals("task,taskName,success,inputSize,ramRequest,peakVmem,peakRss,realtime\n", csv);

        String summary = statistics.summary(0);
        log.info(summary);
    }

    /**
     * Test if the observation values are inserted correctly
     * 
     */
    @Test
    public void testSingleObservations() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());

        Statistics statistics = mockStatistics();
        
        Observation o = Observation.builder()
                .task("task")
                .taskName("taskName")
                .success(true)
                .inputSize(123l)
                .ramRequest(BigDecimal.valueOf(234l))
                .peakVmem(BigDecimal.valueOf(345l))
                .peakRss(BigDecimal.valueOf(456l))
                .realtime(567l)
                .build();
        statistics.addObservation(o);
        
        String csv = statistics.exportCsv(0);
        log.info(csv);
        assertEquals("task,taskName,success,inputSize,ramRequest,peakVmem,peakRss,realtime\n"
                    +"task,taskName,true,123,234,345,456,567\n", csv);

        String summary = statistics.summary(0);
        log.info(summary);
    }

    /**
     * Test if Summary Report is correct
     * 
     */
    @Test
    public void testSummary() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());

        Statistics statistics = mockStatistics();
        
        Observation o1 = Observation.builder()
                .task("task")
                .taskName("taskName")
                .success(true)
                .inputSize(1l)
                .ramRequest(BigDecimal.valueOf(2l))
                .peakVmem(BigDecimal.valueOf(3l))
                .peakRss(BigDecimal.valueOf(4l))
                .realtime(5l)
                .build();
        statistics.addObservation(o1);

        Observation o2 = Observation.builder()
                .task("task")
                .taskName("taskName")
                .success(true)
                .inputSize(2l)
                .ramRequest(BigDecimal.valueOf(3l))
                .peakVmem(BigDecimal.valueOf(4l))
                .peakRss(BigDecimal.valueOf(5l))
                .realtime(6l)
                .build();
        statistics.addObservation(o2);

        Observation o3 = Observation.builder()
                .task("task")
                .taskName("taskName")
                .success(true)
                .inputSize(3l)
                .ramRequest(BigDecimal.valueOf(4l))
                .peakVmem(BigDecimal.valueOf(5l))
                .peakRss(BigDecimal.valueOf(6l))
                .realtime(7l)
                .build();
        statistics.addObservation(o3);

        String csv = statistics.exportCsv(0);
        log.info(csv);
        assertEquals("task,taskName,success,inputSize,ramRequest,peakVmem,peakRss,realtime\n"
                   + "task,taskName,true,1,2,3,4,5\n"
                   + "task,taskName,true,2,3,4,5,6\n"
                   + "task,taskName,true,3,4,5,6,7\n", csv);

        String summary = statistics.summary(0);
        log.info(summary);
        
        String reference = " total observations collected: 3\n"
                + " different tasks: 1\n"
                + " -- task: 'task' --\n"
                + "  named instances of 'task' seen: 1\n"
                + "  success count: 3\n"
                + "  failure count: 0\n"
                + "inputSize  : cnt 3, avr 2.0, min 1, max 3\n"
                + "ramRequest : cnt 3, avr 3.000e+00, min 2.000e+00, max 4.000e+00\n"
                + "peakVmem   : cnt 3, avr 4.000e+00, min 3.000e+00, max 5.000e+00\n"
                + "peakRss    : cnt 3, avr 5.000e+00, min 4.000e+00, max 6.000e+00\n"
                + "realtime   : cnt 3, avr 6.0, min 5, max 7\n";
        assertTrue(summary.endsWith(reference));
    }

}
