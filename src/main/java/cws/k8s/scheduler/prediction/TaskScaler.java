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
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cws.k8s.scheduler.util.Formater.formatBytes;

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

    protected abstract void scaleTask( Task task, Double prediction, long predictorVersion );

    protected abstract Predictor createPredictor( String taskName );

    private void scaleTaskIntern( Task task ) {
        log.debug("1 unscheduledTask: {} {} {}", task.getConfig().getTask(), task.getConfig().getName(),
                formatBytes(task.getOriginalMemoryRequest().longValue()));

        final Predictor predictor = predictors.get( task.getConfig().getTask() );

        final long predictorVersion;
        //Do not predict if the predictor is not set or if the task was already predicted with the same version
        if ( predictor == null || (predictorVersion = predictor.getVersion()) == getTaskVersionForPredictor(task) ) {
            return;
        }

        // query suggestion
        Double prediction = predictor.queryPrediction(task);

        scaleTask( task, prediction, predictorVersion );
    }


    public synchronized void beforeTasksScheduled( final List<Task> unscheduledTasks ) {
        if (!active) {
            return;
        }
        log.debug( unscheduledTasks.size() + " unscheduledTasks" );
        log.debug("--- unscheduledTasks BEGIN ---");
        unscheduledTasks
                .parallelStream()
                .filter( this::applyToThisTask )
                .forEach( this::scaleTaskIntern );
        log.debug("--- unscheduledTasks END ---");
    }

    protected abstract long getTaskVersionForPredictor( Task task );

    public void deactivate(){
        active = false;
    }

    protected Map<String,String> parsePredictorParams( String predictorParams ) {
        final Map<String,String> params = new HashMap<>();
        if ( predictorParams == null ) {
            return params;
        }
        final String[] split = predictorParams.split( "," );
        for ( String s : split ) {
            final String[] split1 = s.split( "=" );
            if ( split1.length != 2 ) {
                throw new IllegalArgumentException( "predictorParams must be a comma separated list of key=value pairs" );
            }
            params.put( split1[0].toLowerCase(), split1[1].toLowerCase() );
        }
        return params;
    }
    
}
