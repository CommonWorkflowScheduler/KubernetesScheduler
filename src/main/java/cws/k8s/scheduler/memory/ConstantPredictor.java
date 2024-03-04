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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import cws.k8s.scheduler.model.Task;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// @formatter:off
/**
 * ConstantPredictor will use the following strategy:
 * 
 * - In case task was successful:
 *   - let the next prediction be 10% higher, then the peakRss was 
 *
 * - In case task has failed:
 *   - reset to initial value
 * 
 * I.e. the suggestions from ConstantPredictor are not dependent on the input
 * size of the tasks.
 * 
 * @author Florian Friederici
 *
 */
// @formatter:on
@Slf4j
@NoArgsConstructor
class ConstantPredictor implements MemoryPredictor {

    private final Map<String, BigDecimal> maxValueByTask = new HashMap<>();
    private final Map<String, Integer> observations = new HashMap<>();
    private final float maxMultiplicand = 0.1F;
    private final float minMultiplicand = 0.05F;

    @Override
    public void addObservation(Observation o) {
        log.debug("ConstantPredictor.addObservation({})", o);
        if (!TaskScaler.checkObservationSanity(o)) {
            log.warn("dismiss observation {}", o);
            return;
        }

        if (Boolean.TRUE.equals(o.success)) {
            // set model to peakRss + 10%
            if ( !maxValueByTask.containsKey(o.task) || o.peakRss.compareTo(maxValueByTask.get(o.task)) > 0 ) {
                maxValueByTask.put(o.task, o.peakRss);
            }
            observations.compute( o.task, ( k, v ) -> v == null ? 1 : v + 1 );
        }

    }

    private BigDecimal getCurrentMultiplicationFactor( String taskName ) {
        final Integer observation = observations.getOrDefault( taskName, 1 );
        if ( observation > 10 ) {
            return BigDecimal.ONE.add( BigDecimal.valueOf( minMultiplicand ) );
        }
        return BigDecimal.ONE.add( BigDecimal.valueOf( minMultiplicand + ( maxMultiplicand - minMultiplicand) / observation ) );
    }

    @Override
    public BigDecimal queryPrediction(Task task) {
        String taskName = task.getConfig().getTask();
        log.debug("ConstantPredictor.queryPrediction({})", taskName);

        if ( task.getConfig().getRepetition() > 0 ) {
            //if this task failed once the old maxValue was likely too small.
            return null;
        }
        if ( maxValueByTask.containsKey(taskName) ) {
            return maxValueByTask.get(taskName)
                    .multiply( getCurrentMultiplicationFactor( task.getConfig().getTask() ) )
                    .setScale( 0, RoundingMode.CEILING );
        } else {
            return null;
        }
    }
}
