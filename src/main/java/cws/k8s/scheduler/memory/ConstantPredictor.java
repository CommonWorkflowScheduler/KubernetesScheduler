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
class ConstantPredictor implements MemoryPredictor {

    Map<String, Integer> generation;
    Map<String, BigDecimal> model;
    Map<String, BigDecimal> initialValue;

    public ConstantPredictor() {
        model = new HashMap<>();
        generation = new HashMap<>();
        initialValue = new HashMap<>();
    }

    @Override
    public void addObservation(Observation o) {
        log.debug("ConstantPredictor.addObservation({})", o);
        if (!TaskScaler.checkObservationSanity(o)) {
            log.warn("dismiss observation {}", o);
            return;
        }

        // store initial ramRequest value per task
        if (!initialValue.containsKey(o.task)) {
            initialValue.put(o.task, o.getRamRequest());
        }
        
        // observations increase the generation value for this task
        if (!generation.containsKey(o.task)) {
            generation.put(o.task, 1);
        } else {
            generation.replace(o.task, 1 + generation.get(o.task));
        }
        
        if (Boolean.TRUE.equals(o.success)) {
            // set model to peakRss + 10%
            if (model.containsKey(o.task)) {
                model.replace(o.task, o.peakRss.multiply(new BigDecimal("1.1")).setScale(0, RoundingMode.CEILING));
            } else {
                model.put(o.task, o.peakRss.multiply(new BigDecimal("1.1")).setScale(0, RoundingMode.CEILING));
            }
        } else {
            // reset to initialValue
            if (model.containsKey(o.task)) {
                model.replace(o.task, this.initialValue.get(o.task));
            } else {
                model.put(o.task, o.ramRequest.multiply(new BigDecimal(2)).setScale(0, RoundingMode.CEILING));
            }
        }

    }

    @Override
    public String queryPrediction(Task task) {
        String taskName = task.getConfig().getTask();
        log.debug("ConstantPredictor.queryPrediction({})", taskName);

//        if (!generation.containsKey(taskName)) {
//            // this taskName is unknown, no prediction possible
//            return null;
//        } else {
//            // only provide a prediction to tasks when the prediction generation is bigger than the tasks generation
//            if (task.getGeneration() >= generation.get(taskName)) {
//                return null;
//            }
//        }

        if (model.containsKey(taskName)) {
            // update the task.generation to the predictor.generation
            task.setGeneration(generation.get(taskName));
            return model.get(taskName).toPlainString();
        } else {
            return null;
        }
    }
}
