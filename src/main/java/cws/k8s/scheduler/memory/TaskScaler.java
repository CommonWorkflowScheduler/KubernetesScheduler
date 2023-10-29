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
import java.util.List;
import java.util.Map;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.Scheduler;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import lombok.extern.slf4j.Slf4j;

/**
 * The TaskScaler offers the interfaces that are used by the Scheduler
 * 
 * It will collect the resource usage results of tasks and change future tasks.
 * 
 * @author Florian Friederici
 */
@Slf4j
public class TaskScaler {

    final KubernetesClient client;
    final Scheduler scheduler;
    final MemoryPredictor memoryPredictor;
    final Statistics statistics;

    public TaskScaler(KubernetesClient client, Scheduler scheduler) {
        this.client = client;
        this.scheduler = scheduler;
        String predictor = System.getenv("MEMORY_PREDICTOR");
        if (predictor == null) {
            predictor = "none";
        }
        switch (predictor.toLowerCase()) {
        case "constant":
            log.debug("using ConstantPredictor");
            this.memoryPredictor = new ConstantPredictor();
            break;

        case "linear":
            log.debug("using LinearPredictor");
            this.memoryPredictor = new LinearPredictor();
            break;

        case "combi":
            log.debug("using CombiPredictor");
            this.memoryPredictor = new CombiPredictor();
            break;

        case "none":
        default:
            log.debug("using NonePredictor");
            this.memoryPredictor = new NonePredictor();
        }
        this.statistics = new Statistics(scheduler,memoryPredictor);
    }

    /**
     * After a task was finished, this method shall be called to collect the tasks
     * resource usage
     * 
     * @param task
     */
    public void afterTaskFinished(Task task) {
        BigDecimal peakRss = NfTrace.getNfPeakRss(task);
        BigDecimal peakVmem = NfTrace.getNfPeakVmem(task);
        long realtime = NfTrace.getNfRealTime(task);
        // @formatter:off
        log.info("taskWasFinished, task={}, name={}, succ={}, inputSize={}, reqRam={}, peak_vmem={}, peak_rss={}, realtime={}",
                task.getConfig().getTask(), 
                task.getConfig().getName(), 
                task.wasSuccessfullyExecuted(),
                task.getInputSize(), 
                task.getPod().getRequest().getRam(), 
                peakVmem,
                peakRss,
                realtime);
        Observation o = Observation.builder()
                .task( task.getConfig().getTask() )
                .taskName( task.getConfig().getName() )
                .success( task.wasSuccessfullyExecuted() )
                .inputSize( task.getInputSize() )
                .ramRequest( task.getPod().getRequest().getRam() )
                .peakVmem(peakVmem)
                .peakRss(peakRss)
                .realtime(realtime)
                .build();
        // @formatter:on
        memoryPredictor.addObservation(o);
        statistics.addObservation(o);

        // TODO this is a workaround, because the SchedulerConfig does not contain the baseDir
        if (statistics.baseDir == null) {
            statistics.baseDir = task.getWorkingDir().substring(0, task.getWorkingDir().lastIndexOf("work"));
        }
    }

    public synchronized void beforeTasksScheduled(final List<Task> unscheduledTasks) {
        log.debug("--- unscheduledTasks BEGIN ---");
        for (Task t : unscheduledTasks) {
            log.debug("1 unscheduledTask: {} {} {}", t.getConfig().getTask(), t.getConfig().getName(),
                    t.getPod().getRequest());

            // if task had no memory request set, it cannot be changed
            if (t.getPod().getRequest().getRam().compareTo(BigDecimal.ZERO) == 0) {
                log.info("cannot change task {}, because it had no prior requirements", t.toString());
                break;
            }
            
            // query suggestion
            String suggestion = memoryPredictor.queryPrediction(t);
            if (suggestion != null) {
                // 1. patch Kubernetes value
                patchTask(t, suggestion);

                // 2. patch CWS value
                List<Container> l = t.getPod().getSpec().getContainers();
                for (Container c : l) {
                    ResourceRequirements req = c.getResources();
                    Map<String, Quantity> limits = req.getLimits();
                    limits.replace("memory", new Quantity(suggestion));
                    Map<String, Quantity> requests = req.getRequests();
                    requests.replace("memory", new Quantity(suggestion));
                    log.debug("container: {}", req);
                }

                log.debug("2 unscheduledTask: {} {} {}", t.getConfig().getTask(), t.getConfig().getName(),
                        t.getPod().getRequest());
            }

        }
        log.debug("--- unscheduledTasks END ---");
    }

    public void afterWorkflow() {
        log.debug("afterWorkflow");
        long timestamp = System.currentTimeMillis();
        statistics.end = timestamp;
        log.info(statistics.summary(timestamp));
        log.debug(statistics.exportCsv(timestamp));
    }

    /**
     * After some testing, this was found to be the only reliable way to patch a pod
     * using the Kubernetes client.
     * 
     * It will create a patch for the memory limits and request values and submit it
     * to the cluster.
     * 
     * @param t          the task to be patched
     * @param suggestion the value to be set
     */
    private void patchTask(Task t, String suggestion) {
        String namespace = t.getPod().getMetadata().getNamespace();
        String podname = t.getPod().getName();
        log.debug("namespace: {}, podname: {}", namespace, podname);
        // @formatter:off
        String patch = "kind: Pod\n"
                + "apiVersion: v1\n"
                + "metadata:\n"
                + "  name: PODNAME\n"
                + "  namespace: NAMESPACE\n"
                + "spec:\n"
                + "  containers:\n"
                + "    - name: PODNAME\n"
                + "      resources:\n"
                + "        limits:\n"
                + "          memory: LIMIT\n"
                + "        requests:\n"
                + "          memory: REQUEST\n"
                + "\n";
        // @formatter:on
        patch = patch.replace("NAMESPACE", namespace);
        patch = patch.replace("PODNAME", podname);
        patch = patch.replace("LIMIT", suggestion);
        patch = patch.replace("REQUEST", suggestion);
        log.debug(patch);

        client.pods().inNamespace(namespace).withName(podname).patch(patch);
    }

    /**
     * This helper checks observations for sanity.
     * 
     * @return true is the Observation looks sane, false otherwise
     */
    public static void checkObservationSanity(Observation o) {
        if (o.task == null || o.taskName == null || o.success == null || o.ramRequest == null || o.peakRss == null) {
            throw new ObservationException("unexpected null value in observation");
        }
        if (o.inputSize < 0) {
            throw new ObservationException("inputSize may not be negative");
        }
        if (o.ramRequest.compareTo(BigDecimal.ZERO) < 0) {
            throw new ObservationException("ramRequest may not be negative");
        }
        if (o.peakRss.compareTo(BigDecimal.ZERO) < 0) {
            throw new ObservationException("peakRss may not be negative");
        }
        if (o.peakRss.compareTo(BigDecimal.ZERO) == 0) {
            throw new ObservationException("peakRss may not be zero (has the .command.trace read failed?)");
        }
    }

}
