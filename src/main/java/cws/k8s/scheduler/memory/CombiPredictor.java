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

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

//@formatter:off
/**
* CombiPredictor will combine predictions made by ConstantPredictor and
* LineraPredictor.
* 
* LinearPredictor fails if there are no inputSize differences to tasks,
* ConstantPredictor can handle this case. So CombiPredictor will run both and
* decide dynamically which predictions to apply.
* 
* @author Florian Friederici
*
*/
//@formatter:on
@Slf4j
public class CombiPredictor implements MemoryPredictor {

    ConstantPredictor constantPredictor;
    LinearPredictor linearPredictor;
    
    public CombiPredictor() {
        this.constantPredictor = new ConstantPredictor();
        this.linearPredictor = new LinearPredictor();
    }

    @Override
    public void addObservation(Observation o) {
        log.debug("CombiPredictor.addObservation({})", o);
        constantPredictor.addObservation(o);
        linearPredictor.addObservation(o);
    }
    
    @Override
    public String queryPrediction(Task task) {
        String taskName = task.getConfig().getTask();
        log.debug("CombiPredictor.queryPrediction({},{})", taskName, task.getInputSize());

        String constantPrediction = constantPredictor.queryPrediction(task);
        String linearPrediction = linearPredictor.queryPrediction(task);
        
        if (constantPrediction==null && linearPrediction==null) {
            // no prediction available at all
            return null;
        }

        if (constantPrediction!=null && linearPrediction==null) {
            // only the constantPrediction is available
            return constantPrediction;
        }

        if (constantPrediction==null && linearPrediction!=null) {
            // only the linearPrediction is available (unusual case)
            return linearPrediction;
        }
        
        log.debug("constantPrediction={}, linearPrediction={}, difference={}", constantPrediction, linearPrediction, new BigDecimal(constantPrediction).subtract(new BigDecimal(linearPrediction)));

        // prefer linearPrediction if both would be available
        return linearPrediction;
    }

}
