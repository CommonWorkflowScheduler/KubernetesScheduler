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

import lombok.Builder;
import lombok.Data;

/**
 * This class holds the observations that can be made after the execution of a
 * task in the workflow. Depending on those observations, either a single one,
 * or multiple, tasks resource needs can be adopted by algorithms.
 * 
 * Note: Would have been an java record if target was java 14+
 * 
 * @author Florian Friederici
 *
 */
@Data
@Builder
public class Observation {

    final String task;
    final String taskName;
    final Boolean success;
    final long inputSize;
    final BigDecimal ramRequest;
    final BigDecimal ramLimit;
    final BigDecimal peakRss;

}
