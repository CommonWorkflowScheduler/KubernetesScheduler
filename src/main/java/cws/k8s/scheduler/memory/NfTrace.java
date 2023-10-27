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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import cws.k8s.scheduler.model.Task;
import lombok.extern.slf4j.Slf4j;

/**
 * Nextflow writes a trace file, when run with "-with-trace" on command line, or
 * "trace.enabled = true" in the configuration file.
 * 
 * This class contains methods to extract values from this traces after the 
 * Tasks have finished.
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
public class NfTrace {

    private NfTrace() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * This method will get the peak resident set size (RSS) from the Nextflow 
     * trace, and return it in BigDecimal format. According to the Nextflow 
     * documentation this is:
     * 
     * "Peak of real memory. This data is read from field VmHWM in /proc/$pid/status file."
     * 
     * https://www.nextflow.io/docs/latest/tracing.html#trace-report
     * 
     * The Linux kernel provides the value in KiB, we multiply with 1024 to have
     * it in byte, like the other values we use.
     * 
     * @return The peak RSS value that this task has used (in byte), BigDecimal.ZERO if extraction failed
     */
    static BigDecimal getNfPeakRss(Task task) {
        String value = extractTraceFile(task, "peak_rss");
        if (value == null) {
            // extraction failed, return ZERO
            return BigDecimal.ZERO;
        } else {
            return new BigDecimal(value).multiply(BigDecimal.valueOf(1024l));
        }
    }

    /**
     * This method will get the realtime value from the Nextflow trace, and 
     * return it as long. According to the Nextflow documentation this is:
     * 
     * "Task execution time i.e. delta between completion and start timestamp."
     * 
     * https://www.nextflow.io/docs/latest/tracing.html#trace-report
     * 
     * @return task execution time (in ms), 0 if extraction failed
     */
    static long getNfRealTime(Task task) {
        String value = extractTraceFile(task, "realtime");
        if (value == null) {
            return 0;
        } else {
            return Long.valueOf(value);
        }
    }

    private static String extractTraceFile(Task task, String key) {
        final String nfTracePath = task.getWorkingDir() + '/' + ".command.trace";
        try {
            Path path = Paths.get(nfTracePath);
            List<String> allLines = Files.readAllLines(path);
            for (String a : allLines) {
                if (a.startsWith(key)) {
                    return a.substring(key.length()+1);
                }
            }
        } catch (Exception e) {
            log.warn("Cannot read nf .command.trace file in " + nfTracePath, e);
        }
        return null;
    }
    
}
