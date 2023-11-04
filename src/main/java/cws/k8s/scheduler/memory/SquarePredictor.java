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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

//@formatter:off
/**
* SquarePredictor will use the following strategy:
* 
* 
* @author Florian Friederici
*
*/
//@formatter:on
@Slf4j
public class SquarePredictor implements MemoryPredictor {

    Map<String, SimpleRegression> model;
    Map<String, Double> overprovisioning;

    public SquarePredictor() {
        model = new HashMap<>();
        overprovisioning = new HashMap<>();
    }

    @Override
    public void addObservation(Observation o) {
        log.warn("not implemented");
        // TODO implement
    }
    
    @Override
    public String queryPrediction(Task task) {
        log.warn("not implemented");
        // TODO implement
        return null;
    }


}
