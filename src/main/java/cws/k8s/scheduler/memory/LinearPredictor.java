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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

//@formatter:off
/**
 * LinearPredictor will use the following strategy:
 * 
 * tbd
 * 
 * @author Florian Friederici
 *
 */
//@formatter:on
@Slf4j
public class LinearPredictor implements MemoryPredictor {
    
    Map<String, List<Observation>> observations;
    Map<String, BigDecimal> suggestions;

    public LinearPredictor() {
        observations = new HashMap<>();
        suggestions = new HashMap<>();
    }
    
    @Override
    public void addObservation(Observation o) {
        log.debug("LinearPredictor.addObservation({})", o);

        // TODO handle success/failure
        
        if (!observations.containsKey(o.taskName)) {
            observations.put(o.taskName, new ArrayList<>());
        }
        
        this.observations.get(o.taskName).add(o);
    }

    @Override
    public String querySuggestion(Task task) {
        String taskName = task.getConfig().getTask();
        log.debug("LinearPredictor.querySuggestion({},{})", taskName, task.getInputSize());

        if (!observations.containsKey(taskName)) {
            log.debug("LinearPredictor has no observations for {}", taskName);
            return null;
        }
        
        List<Observation> obs = observations.get(taskName);
        if (obs.size() < 2) {
            log.debug("LinearPredictor has less than 2 observations for {}", taskName);
            return null;
        }
        
        BigDecimal min_in=null;
        BigDecimal min_rss=null;
        BigDecimal max_in=null;
        BigDecimal max_rss=null;
        for (Observation o : obs) {
            if (max_rss == null || o.peakRss.compareTo(max_rss) > 0) {
                max_rss = o.peakRss;
                max_in = new BigDecimal(o.inputSize);
            }
            if (min_in == null || new BigDecimal(o.inputSize).compareTo(min_in) < 0) {
                min_rss = o.peakRss;
                min_in = new BigDecimal(o.inputSize);
            }
        }
        log.debug("found extremes: ({},{}) ({},{})", min_in, min_rss, max_in, max_rss);
        
        BigDecimal dx = max_in.subtract(min_in);
        BigDecimal dy = max_rss.subtract(min_rss);
        BigDecimal m = dy.divide(dx);
        
        BigDecimal tmp1 = max_in.multiply(min_rss);
        BigDecimal tmp2 = min_in.multiply(max_rss);
        BigDecimal tmp3 = tmp1.subtract(tmp2);
        BigDecimal y0 = tmp3.divide(dx);
        
        log.debug("found slope and y-intercept: {} {})", m, y0);

        BigDecimal expectation = m.multiply(new BigDecimal(task.getInputSize())).add(y0);
        return expectation.toPlainString();
    }

}
