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
import java.util.Map;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.Task;
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
    final MemoryPredictor memoryPredictor;

    public TaskScaler(KubernetesClient client) {
        this.client = client;
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

        case "none":
        default:
            log.debug("using NonePredictor");
            this.memoryPredictor = new NonePredictor();
        }
    }

    /**
     * After a task was finished, this method shall be called to collect the tasks
     * resource usage
     * 
     * @param task
     */
    public void afterTaskFinished(Task task) {
        BigDecimal peakRss = getNfPeakRss(task);
        // @formatter:off
        log.info("taskWasFinished, task={}, name={}, succ={}, inputSize={}, reqRam={}, peak_rss={}",
                task.getConfig().getTask(), 
                task.getConfig().getName(), 
                task.wasSuccessfullyExecuted(),
                task.getInputSize(), 
                task.getPod().getRequest().getRam(), 
                peakRss);
        // TODO Task does not provide Limits value yet
        Observation o = Observation.builder()
                .task( task.getConfig().getTask() )
                .taskName( task.getConfig().getName() )
                .success( task.wasSuccessfullyExecuted() )
                .inputSize( task.getInputSize() )
                .ramRequest( task.getPod().getRequest().getRam() )
                .ramLimit( null )
                .peakRss(peakRss)
                .build();
        // @formatter:on
        memoryPredictor.addObservation(o);
    }

    public void beforeTasksScheduled(final List<Task> unscheduledTasks) {
        synchronized (unscheduledTasks) {
            log.debug("--- unscheduledTasks BEGIN ---");
            for (Task t : unscheduledTasks) {
                log.debug("1 unscheduledTask: {} {} {}", t.getConfig().getTask(), t.getConfig().getName(),
                        t.getPod().getRequest());

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
    }

    public void afterWorkflow() {
        log.debug("afterWorkflow");
        // TODO collect statistics for evaluation
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
        patch = patch.replaceAll("NAMESPACE", namespace);
        patch = patch.replaceAll("PODNAME", podname);
        patch = patch.replaceAll("LIMIT", suggestion);
        patch = patch.replaceAll("REQUEST", suggestion);
        log.debug(patch);

        client.pods().inNamespace(namespace).withName(podname).patch(patch);
    }

    /**
     * Nextflow writes a trace file, when run with "-with-trace" on command line, or
     * "trace.enabled = true" in the configuration file.
     * 
     * This method will get the peak resident set size (RSS) from there, and return
     * it in BigDecimal format.
     * 
     * @return The peak RSS value that this task has used
     */
    private BigDecimal getNfPeakRss(Task task) {
        final String nfTracePath = task.getWorkingDir() + '/' + ".command.trace";
        try {
            Path path = Paths.get(nfTracePath);
            List<String> allLines = Files.readAllLines(path);
            for (String a : allLines) {
                if (a.startsWith("peak_rss")) {
                    BigDecimal peakRss = new BigDecimal(a.substring(9));
                    return peakRss.multiply(BigDecimal.valueOf(1024l));
                }
            }
        } catch (Exception e) {
            log.warn("Cannot read nf .command.trace file in " + nfTracePath, e);
        }
        return BigDecimal.ZERO;
    }

    /**
     * This helper checks observations for sanity.
     * 
     * @return true is the Observation looks sane, false otherwise
     */
    public static void checkObservationSanity(Observation o) {
        if (o.task == null || o.taskName == null || o.success == null || o.ramRequest == null || o.ramLimit == null
                || o.peakRss == null) {
            throw new ObservationException("unexpected null value in observation");
        }
        if (o.inputSize < 0) {
            throw new ObservationException("inputSize may not be negative");
        }
        if (o.ramRequest.compareTo(BigDecimal.ZERO) < 0) {
            throw new ObservationException("ramRequest may not be negative");
        }
        if (o.ramLimit.compareTo(BigDecimal.ZERO) < 0) {
            throw new ObservationException("ramLimit may not be negative");
        }
        if (o.peakRss.compareTo(BigDecimal.ZERO) < 0) {
            throw new ObservationException("peakRss may not be negative");
        }
        if (o.getRamRequest().compareTo(o.ramLimit) > 0) {
            throw new ObservationException("ramRequest is bigger than ramLimit");
        }
    }

}
