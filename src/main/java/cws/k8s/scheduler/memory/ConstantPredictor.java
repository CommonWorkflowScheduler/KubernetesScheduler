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
 * - In case task has failed, double memory 
 * 
 * - In case task was successful, reduce memory according to:
 *     if no old suggestion exists: 
 *       - let the new suggestion be 10% higher, then the peakRss was 
 *     else: 
 *       - new suggestion = (new peakRss + last suggestion) / 2
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

    Map<String, BigDecimal> suggestions;

    public ConstantPredictor() {
        suggestions = new HashMap<>();
    }

    @Override
    public void addObservation(Observation o) {
        log.debug("ConstantPredictor.addObservation({})", o);
        TaskScaler.checkObservationSanity(o);

        if (o.success) {
            // decrease suggestion
            if (suggestions.containsKey(o.task)) {
                BigDecimal sug = (o.peakRss.add(suggestions.get(o.task))).divide(new BigDecimal(2));
                suggestions.replace(o.task, sug.setScale(0, RoundingMode.CEILING));
            } else {
                suggestions.put(o.task, o.peakRss.multiply(new BigDecimal(1.1)).setScale(0, RoundingMode.CEILING));
            }
        } else {
            // increase suggestion
            if (suggestions.containsKey(o.task)) {
                BigDecimal sug = suggestions.get(o.task).multiply(new BigDecimal(2));
                suggestions.replace(o.task, sug.setScale(0, RoundingMode.CEILING));
            } else {
                suggestions.put(o.task, o.ramRequest.multiply(new BigDecimal(2)).setScale(0, RoundingMode.CEILING));
            }
        }

    }

    @Override
    public String queryPrediction(Task task) {
        String taskName = task.getConfig().getTask();
        log.debug("ConstantPredictor.queryPrediction({})", taskName);
        if (suggestions.containsKey(taskName)) {
            return suggestions.get(taskName).toPlainString();
        } else {
            return null;
        }
    }
}
