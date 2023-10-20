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

import cws.k8s.scheduler.model.Task;

/**
 * The MemoryPredictor has two important interfaces:
 * 
 * 1) addObservation() - "add a new observation" after a workflow task is
 * finished, the observation result will be collected in the MemoryPredictor 2)
 * querySuggestion() - "ask for a suggestion" at any time, the MemoryPredictor
 * can be asked what its guess is on the resource requirement of a task
 * 
 * @author Florian Friederici
 *
 */
interface MemoryPredictor {

    /**
     * input observation into the MemoryPredictor, to be used to learn memory usage
     * of tasks to create suggestions
     * 
     * @param o the observation that was made
     */
    void addObservation(Observation o);

    /**
     * ask the MemoryPredictor for a suggestion on how much memory should be
     * assigned to the task.
     * 
     * @param task the task to get a suggestion form
     * @return null, if no suggestion possible, otherwise the value to be used
     */
    String querySuggestion(Task task);

}
