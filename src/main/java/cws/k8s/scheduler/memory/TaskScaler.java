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
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import lombok.extern.slf4j.Slf4j;

/** The TaskScaler offers the interfaces that are used by the Scheduler
 * 
 * It will collect the resource usage results of tasks and change future tasks.
 * 
 * @author Florian Friederici
 */
@Slf4j
public class TaskScaler {
	
    final KubernetesClient client;
    final MemoryOptimizer memoryOptimizer;

	public TaskScaler(KubernetesClient client) {
		this.client = client;
		this.memoryOptimizer = new cws.k8s.scheduler.memory.MemoryOptimizer();
	}

	/** After a task was finished, this method shall be called to collect the 
	 * tasks resource usage
	 * 
	 * @param task
	 */
	public void afterTaskFinished(Task task) {
		BigDecimal peakRss = getNfPeakRss(task);
		
        log.info("taskWasFinished, task={}, name={}, succ={}, inputSize={}, reqRam={}, peak_rss={}, wasted={}" ,
        		task.getConfig().getTask(),
        		task.getConfig().getName(),
        		task.wasSuccessfullyExecuted(), 
        		task.getInputSize(), 
        		task.getPod().getRequest().getRam(),
        		peakRss,
        		task.getPod().getRequest().getRam().subtract(peakRss)
        );
        memoryOptimizer.addObservation(new cws.k8s.scheduler.memory.Observation(
        		task.getConfig().getTask(),
        		task.getConfig().getName(),
        		task.wasSuccessfullyExecuted(), 
        		task.getInputSize(), 
        		task.getPod().getRequest().getRam(),
        		null,
        		peakRss,
        		task.getPod().getRequest().getRam().subtract(peakRss))
        		);

	}
	
	public void beforeTasksScheduled(final List<Task> unscheduledTasks) {
        synchronized(unscheduledTasks) {
        	log.debug("--- unscheduledTasks BEGIN ---");
            for (Task t : unscheduledTasks) {
            	log.debug("1 unscheduledTask: {} {} {}", t.getConfig().getTask(), t.getConfig().getName(), t.getPod().getRequest());
            	            	
            	// query suggestion
            	String suggestion = memoryOptimizer.querySuggestion(t.getConfig().getTask());
            	if (suggestion != null) {
                	// 1. patch kubernetes value
            		patchTask(t, suggestion);

                	// 2. patch cws value
                	List<Container> l = t.getPod().getSpec().getContainers();
                	for (Container c : l) {
                		ResourceRequirements req = c.getResources();
                    	Map<String, Quantity> limits = req.getLimits();
                    	limits.replace("memory", new Quantity(suggestion));
                    	Map<String, Quantity> requests = req.getRequests();
                    	requests.replace("memory", new Quantity(suggestion));
                    	log.debug("container: {}", req);
                	}
                	
                	/*
                	InputStream patchStream = new ByteArrayInputStream(patch.getBytes());
                	Pod patchedPod = client.pods().load(patchStream).item();
                	client.pods().inNamespace(namespace).createOrReplace(patchedPod);
                	*/            	
                	log.debug("2 unscheduledTask: {} {} {}", t.getConfig().getTask(), t.getConfig().getName(), t.getPod().getRequest());
            	}
            	
            }
        	log.debug("--- unscheduledTasks END ---");
        }
	}

	/** After some testing, this was found to be the only reliable way to patch
	 * a pod using the Kubernetes client.
	 * 
	 * It will create a patch for the memory limits and request values and
	 * submit it to the cluster.
	 * 
	 * @param t the task to be patched
	 * @param suggestion the value to be set
	 */
	private void patchTask(Task t, String suggestion) {
    	String namespace = t.getPod().getMetadata().getNamespace();
    	String podname = t.getPod().getName();
    	log.debug("namespace: {}, podname: {}", namespace, podname);
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
    	patch = patch.replaceAll("NAMESPACE", namespace);
    	patch = patch.replaceAll("PODNAME", podname);
    	patch = patch.replaceAll("LIMIT", suggestion);
    	patch = patch.replaceAll("REQUEST", suggestion);
    	log.debug(patch);

    	client.pods().inNamespace(namespace).withName(podname).patch(patch);
	}
	
    /** Nextflow writes a trace file, when run with "-with-trace" on command 
     * line, or "trace.enabled = true" in the configuration file.
     * 
     * This method will get the peak resident set size (RSS) from there, and
     * return it in BigDecimal format.
     *  
     * @return The peak RSS value that this task has used
     */
    private BigDecimal getNfPeakRss(Task task) {
        final String nfTracePath = task.getWorkingDir() + '/' + ".command.trace";
    	try {
    		java.nio.file.Path path = java.nio.file.Paths.get(nfTracePath);
    		java.util.List<String> allLines = java.nio.file.Files.readAllLines(path);
    	    for (String a: allLines) {
    	    	if (a.startsWith("peak_rss")) {
    	    		BigDecimal peakRss = new BigDecimal(a.substring(9));
    	    		return peakRss.multiply(BigDecimal.valueOf(1024l));
    	    	}
    	    }
        } catch ( Exception e ){
            log.warn( "Cannot read nf .command.trace file in " + nfTracePath, e );
        }
    	return BigDecimal.ZERO;
    }
	
}
