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

import org.apache.commons.math3.stat.regression.SimpleRegression;

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

//@formatter:off
/**
 * LinearPredictor will use the following strategy:
 * 
 * If there are less than 2 observations, give no prediction, else:
 * Calculate linear regression model and provide predictions.
 * 
 * Predictions start with 10% over-provisioning. If tasks fail, this will
 * increase automatically.
 * 
 * @author Florian Friederici
 *
 */
//@formatter:on
@Slf4j
public class LinearPredictor implements MemoryPredictor {
    
    Map<String, SimpleRegression> model;
    Map<String, Double> overprovisioning;
    
    public LinearPredictor() {
        model = new HashMap<>();
        overprovisioning = new HashMap<>();
    }
    
    @Override
    public void addObservation(Observation o) {
        log.debug("LinearPredictor.addObservation({})", o);
        TaskScaler.checkObservationSanity(o);

        if (!overprovisioning.containsKey(o.task)) {
            overprovisioning.put(o.task, 1.1);
        }
        
        if (o.success) {
            if (!model.containsKey(o.task)) {
                model.put(o.task, new SimpleRegression());
            }
            
            double x = o.getInputSize();
            double y = o.getPeakRss().doubleValue();
            model.get(o.task).addData(x,y);
        } else {
            log.debug("overprovisioning value will increase due to task failure");
            Double old = overprovisioning.get(o.task);
            overprovisioning.put(o.task, old+0.01);
        }
    }

    @Override
    public String queryPrediction(Task task) {
        String taskName = task.getConfig().getTask();
        log.debug("LinearPredictor.queryPrediction({},{})", taskName, task.getInputSize());

        if (!model.containsKey(taskName)) {
            log.debug("LinearPredictor has no model for {}", taskName);
            return null;
        }
        
        SimpleRegression simpleRegression = model.get(taskName);
        double prediction = simpleRegression.predict(task.getInputSize());

        if (Double.isNaN(prediction)) {
            log.debug("No prediction possible for {}", taskName);
            return null;
        }

        if (prediction < 0) {
            log.warn("prediction would be negative: {}", prediction);
            return null;
        }

        return new BigDecimal(prediction).multiply(new BigDecimal(overprovisioning.get(taskName))).setScale(0, RoundingMode.CEILING).toPlainString();
    }

}
