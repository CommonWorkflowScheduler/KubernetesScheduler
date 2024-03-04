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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * JUnit 4 Tests for the NfTrace utility class
 * 
 * @author Florian Friederici
 * 
 */
@Slf4j
public class NfTraceTest {

    String exampleTrace = "nextflow.trace/v2\n"
            + "realtime=30090\n"
            + "%cpu=812\n"
            + "cpu_model=Intel(R) Core(TM) i7-8550U CPU @ 1.80GHz\n"
            + "rchar=79901\n"
            + "wchar=427\n"
            + "syscr=276\n"
            + "syscw=22\n"
            + "read_bytes=380928\n"
            + "write_bytes=0\n"
            + "%mem=71\n"
            + "vmem=728216\n"
            + "rss=581672\n"
            + "peak_vmem=728216\n"
            + "peak_rss=593192\n"
            + "vol_ctxt=1906\n"
            + "inv_ctxt=11615";
    
    /**
     * mock a Task with the trace string as trace file
     * 
     * @param trace the contents for the trace file, if null no file will be created
     * @return the mocked Task
     * @throws IOException if file i/o goes wrong
     */
    private Task mockTask(String trace) throws IOException {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        Task task = MemoryPredictorTest.createTask("taskName", 0);
        TaskConfig taskConfig = (TaskConfig)ReflectionTestUtils.getField(task, "config");
        Path tmpdir = Files.createTempDirectory("unittest.");
        tmpdir.toFile().deleteOnExit();
        ReflectionTestUtils.setField(taskConfig, "workDir", tmpdir.toFile().getAbsolutePath());
        if (trace != null) {
            String filename = ".command.trace";
            Path path = Paths.get(tmpdir.toFile().getAbsolutePath() + File.separator + filename);
            Files.write(path, trace.getBytes());
        }
        return task;
    }
    
    /**
     * Positive test case for NfTrace.getNfPeakRss
     * 
     * @throws IOException
     */
    @Test
    public void testGetNfPeakRss() throws IOException {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        
        Task task = mockTask(exampleTrace);
        log.info("workdir: {}", task.getWorkingDir());

        BigDecimal peakRss = NfTrace.getNfPeakRss(task);
        log.info("" + peakRss);
        assertEquals(0,peakRss.compareTo(new BigDecimal("607428608")));
    }

    /**
     * Negative test case for NfTrace.getNfPeakRss
     * When the trace file is missing, we expect BigDecimal.ZERO as peakRss
     * 
     * @throws IOException
     */
    @Test
    public void testGetNfPeakRssMissingFile() throws IOException {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        
        Task task = mockTask(null);
        log.info("workdir: {}", task.getWorkingDir());

        BigDecimal peakRss = NfTrace.getNfPeakRss(task);
        log.info("" + peakRss);
        assertEquals(0,peakRss.compareTo(BigDecimal.valueOf(-1)));
    }
    
    /**
     * Positive test case for NfTrace.getNfRealTime
     * 
     * @throws IOException
     */
    @Test
    public void testGetNfRealTime() throws IOException {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        Task task = mockTask(exampleTrace);
        log.info("workdir: {}", task.getWorkingDir());
        
        long realtime = NfTrace.getNfRealTime(task);
        log.info("" + realtime);
        assertEquals(30090,realtime);
    }

    /**
     * Negative test case for NfTrace.getNfRealTime
     * 
     * @throws IOException
     */
    @Test
    public void testGetNfRealTimeMissingFile() throws IOException {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        Task task = mockTask(null);
        log.info("workdir: {}", task.getWorkingDir());
        
        long realtime = NfTrace.getNfRealTime(task);
        log.info("" + realtime);
        assertEquals(-1,realtime);
    }

    /**
     * Positive test case for NfTrace.getNfPeakVmem
     * 
     * @throws IOException
     */
    @Test
    public void testGetNfPeakVmem() throws IOException {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        Task task = mockTask(exampleTrace);
        log.info("workdir: {}", task.getWorkingDir());
        
        BigDecimal peakVmem = NfTrace.getNfPeakVmem(task);
        log.info("" + peakVmem);
        assertEquals(0,peakVmem.compareTo(BigDecimal.valueOf(745693184)));
    }

    /**
     * Negative test case for NfTrace.getNfPeakVmem
     * 
     * @throws IOException
     */
    @Test
    public void testGetNfPeakVmemMissingFile() throws IOException {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        Task task = mockTask(null);
        log.info("workdir: {}", task.getWorkingDir());
        
        BigDecimal peakVmem = NfTrace.getNfPeakVmem(task);
        log.info("" + peakVmem);
        assertEquals(0,peakVmem.compareTo(BigDecimal.valueOf(-1)));
    }

}
