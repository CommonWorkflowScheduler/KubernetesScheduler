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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;

import cws.k8s.scheduler.scheduler.Scheduler;
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
    
    String baseDir;
    final Scheduler scheduler;
    final MemoryPredictor memoryPredictor;
    long start;
    long end;

    boolean active = true;
    List<Observation> observations = new ArrayList<>();
    
    public Statistics(Scheduler scheduler, MemoryPredictor memoryPredictor) {
        this.scheduler = scheduler;
        this.memoryPredictor = memoryPredictor;
        String disableStatistics = System.getenv("DISABLE_STATISTICS");
        if (disableStatistics != null) {
            active = false;
        }
        this.start = System.currentTimeMillis();
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
     * Save all Observations into csv file in baseDir
     * 
     * @param timestamp time stamp for the file name
     * @return the csv as string for logging
     */
    String exportCsv(long timestamp) {
        if (!active) {
            log.info("Statistics disabled by environment variable");
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("task,taskName,success,inputSize,ramRequest,peakVmem,peakRss,realtime\n");
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
            sb.append(o.getPeakVmem().toPlainString());
            sb.append(",");
            sb.append(o.getPeakRss().toPlainString());
            sb.append(",");
            sb.append(o.getRealtime());
            sb.append("\n");
        }
        String csv = sb.toString();
        
        if (baseDir != null) {
            Path path = Paths.get(baseDir + "TaskScaler_" + timestamp + ".csv");
            log.debug("save csv to: {}", path);
            try {
                Files.write(path, csv.getBytes());
            } catch (IOException e) {
                log.warn("could not save statistics csv to {}", path);
            }
        } else {
            log.debug("baseDir was not set, could not save csv file");
        }
         
        return csv;
    }

    /**
     * Save summary to file in baseDir
     * 
     * @param timestamp time stamp for the filename
     * @return the summary as string for logging
     */
    String summary(long timestamp) {
        if (!active) {
            log.info("Statistics disabled by environment variable");
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("~~~ Statistics ~~~\n");
        sb.append(" execution: ");
        sb.append(this.scheduler.getExecution());
        sb.append("\n");
        sb.append(" memory predictor: ");
        sb.append(this.memoryPredictor.getClass());
        sb.append("\n");
        sb.append(" start: ");
        sb.append(String.valueOf(new Date(this.start)));
        sb.append(" | end: ");
        sb.append(String.valueOf(new Date(this.end)));
        sb.append("\n makespan: ");
        sb.append(this.end-this.start);
        sb.append(" ms\n");
        sb.append(" total observations collected: ");
        sb.append(observations.size());
        sb.append("\n");

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
                ts.peakVmemStatistics.accept(o.peakVmem.doubleValue());
                ts.peakRssStatistics.accept(o.peakRss.doubleValue());
                ts.realtimeStatistics.accept(o.realtime);
            } else {
                ts.failCount++;
            }
        }

        sb.append(" different tasks: ");
        sb.append(tasks.size());
        sb.append("\n");
        
        for (String task : tasks) {
            TaskSummary ts = taskSummaryMap.get(task);
            sb.append(" -- task: '");
            sb.append(task);
            sb.append("' --\n");
            sb.append("  named instances of '");
            sb.append(task);
            sb.append("' seen: ");
            sb.append(taskMap.get(task).size());
            sb.append("\n");
            sb.append("  success count: ");
            sb.append(ts.successCount);
            sb.append("\n");
            sb.append("  failure count: ");
            sb.append(ts.failCount);
            sb.append("\n");
            // @formatter:off
            sb.append(String.format(Locale.US, "inputSize  : cnt %d, avr %.1f, min %d, max %d%n",
                    ts.inputSizeStatistics.getCount(),
                    ts.inputSizeStatistics.getAverage(),
                    ts.inputSizeStatistics.getMin(),
                    ts.inputSizeStatistics.getMax()) );
            sb.append(String.format(Locale.US, "ramRequest : cnt %d, avr %.3e, min %.3e, max %.3e%n",
                    ts.ramRequestStatitistics.getCount(),
                    ts.ramRequestStatitistics.getAverage(),
                    ts.ramRequestStatitistics.getMin(),
                    ts.ramRequestStatitistics.getMax()) );
            sb.append(String.format(Locale.US, "peakVmem   : cnt %d, avr %.3e, min %.3e, max %.3e%n",
                    ts.peakVmemStatistics.getCount(),
                    ts.peakVmemStatistics.getAverage(),
                    ts.peakVmemStatistics.getMin(),
                    ts.peakVmemStatistics.getMax()) );
            sb.append(String.format(Locale.US, "peakRss    : cnt %d, avr %.3e, min %.3e, max %.3e%n",
                    ts.peakRssStatistics.getCount(),
                    ts.peakRssStatistics.getAverage(),
                    ts.peakRssStatistics.getMin(),
                    ts.peakRssStatistics.getMax()) );
            sb.append(String.format(Locale.US, "realtime   : cnt %d, avr %.1f, min %d, max %d%n",
                    ts.realtimeStatistics.getCount(),
                    ts.realtimeStatistics.getAverage(),
                    ts.realtimeStatistics.getMin(),
                    ts.realtimeStatistics.getMax()) );
            // @formatter:on
        }

        String summary = sb.toString();
        if (baseDir != null) {
            Path path = Paths.get(baseDir + "TaskScaler_" + timestamp + ".txt");
            log.debug("save summary to: {}", path);
            try {
                Files.write(path, summary.getBytes());
            } catch (IOException e) {
                log.warn("could not save statistics summary to {}", path);
            }
        } else {
            log.debug("baseDir was not set, could not save summary file");
        }
        return summary;
    }

    @Data
    class TaskSummary {
        final String task;
        int successCount = 0;
        int failCount = 0;
        LongSummaryStatistics inputSizeStatistics = new LongSummaryStatistics();
        DoubleSummaryStatistics ramRequestStatitistics = new DoubleSummaryStatistics();
        DoubleSummaryStatistics peakVmemStatistics = new DoubleSummaryStatistics();
        DoubleSummaryStatistics peakRssStatistics = new DoubleSummaryStatistics();
        LongSummaryStatistics realtimeStatistics = new LongSummaryStatistics();
    }
}
