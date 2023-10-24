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

import java.util.Arrays;
import java.util.List;

import org.springframework.test.util.ReflectionTestUtils;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;

/**
 * Common methods for all MemoryPredictor Tests
 * 
 * @author Florian Friederici
 * 
 */
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

}
