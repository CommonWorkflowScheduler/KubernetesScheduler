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

import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.Scheduler;
import lombok.extern.slf4j.Slf4j;

/**
 * JUnit 5 Tests for the TaskScaler class
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
class TaskScalerTest {
    
    private Task mockTask() {
        Requirements r = Mockito.mock(Requirements.class);
        when(r.getRam()).thenReturn(BigDecimal.ONE);
        
        PodWithAge p = Mockito.mock(PodWithAge.class);
        when(p.getRequest()).thenReturn(r);
        
        NodeWithAlloc n = Mockito.mock(NodeWithAlloc.class);
        when(n.getName()).thenReturn("nodename");
        
        TaskConfig tc = Mockito.mock(TaskConfig.class);
        when(tc.getName()).thenReturn("Unittest");
        when(tc.getTask()).thenReturn("task");
        when(tc.getName()).thenReturn("task (1)");

        Task t = Mockito.mock(Task.class);
        when(t.getConfig()).thenReturn(tc);
        when(t.wasSuccessfullyExecuted()).thenReturn(true);
        when(t.getInputSize()).thenReturn(1l);
        when(t.getPod()).thenReturn(p);
        when(t.getNode()).thenReturn(n);
        
        // TODO provide .command.trace file
        when(t.getWorkingDir()).thenReturn("work");
        
        return t;
    }
    
    private TaskScaler mockTaskScaler() {
        SchedulerConfig schedulerConfig = Mockito.mock(SchedulerConfig.class);
        ReflectionTestUtils.setField(schedulerConfig, "memoryPredictor", "none");        
        Scheduler scheduler = Mockito.mock(Scheduler.class);
        KubernetesClient kubernetesClient = Mockito.mock(KubernetesClient.class);
        
        BigDecimal maxCpu = BigDecimal.ONE;
        BigDecimal maxRam = BigDecimal.TEN;
        Requirements requirements = new Requirements( maxCpu, maxRam);
        NodeWithAlloc nwa = Mockito.mock(NodeWithAlloc.class);
        ReflectionTestUtils.setField(nwa, "maxResources", requirements);
        ReflectionTestUtils.setField(nwa, "assignedPods", new HashMap<>());
        when(nwa.getMaxResources()).thenReturn(requirements);
        when(nwa.getAvailableResources()).thenReturn(requirements);
        
        List<NodeWithAlloc> allNodes = new ArrayList<NodeWithAlloc>();
        allNodes.add(nwa);
        when(kubernetesClient.getAllNodes()).thenReturn(allNodes);

        TaskScaler ts = new TaskScaler(scheduler, schedulerConfig, kubernetesClient);
        return ts;
    }

    /**
     * Test NonePredictor overhead
     */
    @Test
    void testAfterTaskFinished() {
        TaskScaler ts = mockTaskScaler();
        Task t = mockTask();

        long repetitions = 1000;
        long startTime = System.currentTimeMillis();
        for (int i=0; i<repetitions; i++) {
            ts.afterTaskFinished(t);            
        }
        long endTime = System.currentTimeMillis();
        
        log.info("duration was: {} ms", endTime-startTime);
        log.info("that are: {} ms per operation", (endTime-startTime)/(float)repetitions);
    }

    /**
     * Test NonePredictor overhead
     */
    @Test
    void testBeforeTasksScheduled() {
        TaskScaler ts = mockTaskScaler();
        Task t = mockTask();
        List<Task> unscheduled = new ArrayList<>();
        unscheduled.add(t);
        
        long repetitions = 1000;
        long startTime = System.currentTimeMillis();
        for (int i=0; i<repetitions; i++) {
            ts.beforeTasksScheduled(unscheduled);
        }
        long endTime = System.currentTimeMillis();
        
        log.info("duration was: {} ms", endTime-startTime);
        log.info("that are: {} ms per operation", (endTime-startTime)/(float)repetitions);
    }

}
