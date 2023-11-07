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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

//@formatter:off
/**
* WaryPredictor will use the following strategy:
* 
* If there are less than 3 observations, give no prediction, else:
* Calculate linear regression model and test if all observations would fit into
* the model. If all past observations fit into the model, give a prediction.
* If the model does not fit the past observations, provide initial value.
* 
* Predictions start with 10% over-provisioning. If tasks fail, this will
* increase automatically.
* 
* WaryPredictor will never exceed the initial value.
* 
* @author Florian Friederici
*
*/
//@formatter:on
@Slf4j
public class WaryPredictor implements MemoryPredictor {

    Map<String, SimpleRegression> model;
    Map<String, Double> overprovisioning;
    Map<String, List<Pair<Double, Double>>> observations;
    Map<String, Integer> errorCounter;
    Map<String, BigDecimal> initialValue;
    
    public WaryPredictor() {
        model = new HashMap<>();
        overprovisioning = new HashMap<>();
        observations = new HashMap<>();
        errorCounter = new HashMap<>();
        initialValue = new HashMap<>();
    }

    @Override
    public void addObservation(Observation o) {
        log.debug("WaryPredictor.addObservation({})", o);
        if (!TaskScaler.checkObservationSanity(o)) {
            log.warn("dismiss observation {}", o);
            return;
        }

        // store initial ramRequest value per task
        if (!initialValue.containsKey(o.task)) {
            initialValue.put(o.task, o.getRamRequest());
        }

        if (!overprovisioning.containsKey(o.task)) {
            overprovisioning.put(o.task, 1.1);
        }

        if (!errorCounter.containsKey(o.task)) {
            errorCounter.put(o.task, 0);
        }

        if (Boolean.TRUE.equals(o.success)) {

            if (!observations.containsKey(o.task)) {
                observations.put(o.task, new ArrayList<>());
            }
            if (!model.containsKey(o.task)) {
                model.put(o.task, new SimpleRegression());
            }

            double x = o.getInputSize();
            double y = o.getPeakRss().doubleValue();

            observations.get(o.task).add(Pair.of(x, y));
            model.get(o.task).addData(x,y);
        } else {
            Integer errors = errorCounter.get(o.task);
            errorCounter.put(o.task, 1+errors);
            log.debug("overprovisioning value will increase due to task failure, errors: {}", 1+errors);
            Double old = overprovisioning.get(o.task);
            overprovisioning.put(o.task, old+0.05);
        }
    }

    @Override
    public String queryPrediction(Task task) {
        String taskName = task.getConfig().getTask();
        log.debug("WaryPredictor.queryPrediction({},{})", taskName, task.getInputSize());

        if (!model.containsKey(taskName)) {
            log.debug("WaryPredictor has no model for {}", taskName);
            return null;
        }
        
        if (2 < errorCounter.get(taskName)) {
            log.warn("to many errors for {}, providing initial value", taskName);
            return initialValue.get(taskName).toPlainString();
        }
        
        SimpleRegression simpleRegression = model.get(taskName);

        if (simpleRegression.getN() < 3) {
            log.debug("Not enough observations for {}", taskName);
            return null;
        }

        // would the model match the past successful observations?
        List<Pair<Double, Double>> observationList = observations.get(taskName);
        for (Pair<Double, Double> o : observationList) {
            double p = simpleRegression.predict(o.getLeft());
            double op = overprovisioning.get(taskName);
            if ( (p*op) < o.getRight() ) {
                // The model predicted value would have been smaller then the 
                // observed value. Our model is not (yet) appropriate.
                // Increase overprovisioning
                log.debug("overprovisioning value will increase due to model mismatch");
                Double old = overprovisioning.get(taskName);
                overprovisioning.put(taskName, old+0.05);
                // Don't make a prediction this time
                return null;
            }
        }
        
        double prediction = simpleRegression.predict(task.getInputSize());

        if (Double.isNaN(prediction)) {
            log.debug("No prediction possible for {}", taskName);
            return null;
        }

        if (prediction < 0) {
            log.warn("prediction would be negative: {}", prediction);
            return null;
        }
        
        if (prediction > initialValue.get(taskName).doubleValue()) {
            log.warn("prediction would exceed initial value");
            return initialValue.get(taskName).toPlainString();
        }

        return BigDecimal.valueOf(prediction).multiply(BigDecimal.valueOf(overprovisioning.get(taskName))).setScale(0, RoundingMode.CEILING).toPlainString();
    }
    
    
}
