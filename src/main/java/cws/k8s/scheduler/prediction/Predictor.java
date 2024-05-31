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

package cws.k8s.scheduler.prediction;

import cws.k8s.scheduler.model.Task;

// @formatter:off
/**
 * The Predictor has two important interfaces:
 * 
 * 1) addTask()
 *    - "add a new task" after a workflow task is finished, the
 *    observation result will be collected in the Predictor
 * 
 * 2) queryPrediction() 
 *    - "ask for a suggestion" at any time, the Predictor can be asked
 *    what its guess is on the resource requirement of a task
 * 
 * Different strategies can be tried and exchanged easily, they just have to 
 * implement those two interfaces. See ConstantPredictor and LinearPredictor
 * for concrete strategies.
 * 
 * @author Florian Friederici
 *
 */
// @formatter:on
public interface Predictor {

    /**
     * input observation into the Predictor, to learn a value
     * of tasks to create suggestions
     * 
     * @param t the task to be observed
     */
    void addTask( Task t );

    /**
     * ask the Predictor for a suggestion on how resources should be
     * assigned to the task.
     * 
     * @param task the task to get a suggestion form
     * @return null, if no suggestion possible, otherwise the value to be used
     */
    Double queryPrediction( Task task );

    double getDependentValue( Task task );

    double getIndependentValue( Task task );

    long getVersion();

}
