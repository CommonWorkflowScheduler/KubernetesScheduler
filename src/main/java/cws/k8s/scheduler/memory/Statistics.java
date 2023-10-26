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

import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * This class collects all observations and provides a statistics summary
 * 
 * Statistics can be disabled via environment variable DISABLE_STATISTICS
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
public class Statistics {

    boolean active = true;
    List<Observation> observations = new ArrayList<>();
    
    public Statistics() {
        String disableStatistics = System.getenv("DISABLE_STATISTICS");
        if (disableStatistics != null) {
            active = false;
        }
    }
    
    /**
     * Collect Observations for statistics
     * 
     * @param o the observation that was made
     */
    void addObservation(Observation o) {
        if (active) {
            observations.add(o);
        }
    }

    /**
     * Save all Observations into csv file
     * 
     */
    String exportCsv() {
        StringBuilder sb = new StringBuilder();
        sb.append("task,taskName,success,inputSize,ramRequest,peakRss\n");
        for (Observation o : observations) {
            sb.append(o.getTask());
            sb.append(",");
            sb.append(o.getTaskName());
            sb.append(",");
            sb.append(o.getSuccess());
            sb.append(",");
            sb.append(o.getInputSize());
            sb.append(",");
            sb.append(o.getRamRequest().toPlainString());
            sb.append(",");
            sb.append(o.getPeakRss().toPlainString());
            sb.append("\n");
        }
        return sb.toString();
    }
    
    /**
     * Print summary to log.info
     * 
     */
    void summary() {
        if (!active) {
            log.info("Statistics disabled by environment variable");
            return;
        }
        log.info("~~~ Statistics ~~~");
        log.info(" total observations collected: {}", observations.size());

        Set<String> tasks = new HashSet<>();
        Map<String, Set<String>> taskMap = new HashMap<>();
        Map<String, TaskSummary> taskSummaryMap = new HashMap<>();
        for (Observation o : observations) {
            tasks.add(o.task);
            if (!taskMap.containsKey(o.task)) {
                taskMap.put(o.task, new HashSet<>());
            }
            taskMap.get(o.task).add(o.taskName);
            if (!taskSummaryMap.containsKey(o.task)) {
                taskSummaryMap.put(o.task, new TaskSummary(o.task));
            }
            TaskSummary ts = taskSummaryMap.get(o.task);
            if (Boolean.TRUE.equals(o.success)) {
                ts.successCount++;
                ts.inputSizeStatistics.accept(o.inputSize);
                // TODO check if BigDecimal is bigger than double can handle
                ts.ramRequestStatitistics.accept(o.ramRequest.doubleValue());
                ts.peakRssStatistics.accept(o.peakRss.doubleValue());
            } else {
                ts.failCount++;
            }
        }

        log.info(" different tasks: {}", tasks.size());
        
        for (String task : tasks) {
            TaskSummary ts = taskSummaryMap.get(task);
            log.info(" -- task: '{}' --", task);
            log.info("  named instances of '{}' seen: {}", task, taskMap.get(task).size());
            log.info("  success count: {}", ts.successCount);
            log.info("  failure count: {}", ts.failCount);
            // @formatter:off
            String fstr = "%.3e";
            log.info("  inputSize : cnt {}, avr {}, min {}, max {}", 
                    ts.inputSizeStatistics.getCount(), 
                    String.format("%.1f",ts.inputSizeStatistics.getAverage()),
                    String.format("%d",ts.inputSizeStatistics.getMin()),
                    String.format("%d",ts.inputSizeStatistics.getMax()));
            log.info("  ramRequest: cnt {}, avr {}, min {}, max {}", 
                    ts.ramRequestStatitistics.getCount(), 
                    String.format(fstr,ts.ramRequestStatitistics.getAverage()),
                    String.format(fstr,ts.ramRequestStatitistics.getMin()),
                    String.format(fstr,ts.ramRequestStatitistics.getMax()));
            log.info("  peakRss   : cnt {}, avr {}, min {}, max {}", 
                    ts.peakRssStatistics.getCount(), 
                    String.format(fstr,ts.peakRssStatistics.getAverage()),
                    String.format(fstr,ts.peakRssStatistics.getMin()),
                    String.format(fstr,ts.peakRssStatistics.getMax()));
            // @formatter:on

        }
    }

    @Data
    class TaskSummary {
        final String task;
        int successCount = 0;
        int failCount = 0;
        LongSummaryStatistics inputSizeStatistics = new LongSummaryStatistics();
        DoubleSummaryStatistics ramRequestStatitistics = new DoubleSummaryStatistics();
        DoubleSummaryStatistics peakRssStatistics = new DoubleSummaryStatistics();
    }
}
