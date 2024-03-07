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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

/**
 * The TaskScaler offers the interfaces that are used by the Scheduler
 * 
 * It will collect the resource usage results of tasks and change future tasks.
 * 
 * @author Florian Friederici
 */
@Slf4j
public abstract class TaskScaler {

    protected boolean active = true;
    protected final Map<String, Predictor> predictors = new HashMap<>();

    /**
     * After a task was finished, this method shall be called to collect the tasks
     * resource usage
     *
     * @param task that was finished
     */
    public void afterTaskFinished( Task task) {
        if (!active) {
            return;
        }
        if ( !isValid( task ) ) {
            return;
        }
        synchronized ( predictors ) {
            final Predictor predictor = predictors.computeIfAbsent( task.getConfig().getTask(), this::createPredictor );
            predictor.addTask( task );
        }
    }

    /**
     * This method checks if the data is valid and the model should be trained using it.
     * @param task that was finished
     * @return true if the data is valid
     */
    protected abstract boolean isValid( Task task );

    protected boolean applyToThisTask( Task task ) {
        return true;
    }

    protected abstract void scaleTask( Task task );

    protected abstract Predictor createPredictor( String taskName );


    public synchronized void beforeTasksScheduled( final List<Task> unscheduledTasks ) {
        if (!active) {
            return;
        }
        log.debug("--- unscheduledTasks BEGIN ---");
        unscheduledTasks
                .parallelStream()
                .filter( this::applyToThisTask )
                .forEach( this::scaleTask );
        log.debug("--- unscheduledTasks END ---");
    }

    public void deactivate(){
        active = false;
    }
    
}
