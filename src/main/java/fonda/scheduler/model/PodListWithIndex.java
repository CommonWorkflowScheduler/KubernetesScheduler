package fonda.scheduler.model;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PodListWithIndex extends PodList {

    private final Map<String, Pod> nameIndexMap = new HashMap<>();

    public PodListWithIndex() {}

    public PodListWithIndex(List<Pod> pods) {
        pods.forEach(this::addPodToList);
    }

    /**
     * Overrides existing pod if pod is already in list
     *
     * @param pod
     */
    public void addPodToList(Pod pod) {
        nameIndexMap.put(pod.getMetadata().getName(), pod);
    }

    public void removePodFromList(Pod pod) {
        nameIndexMap.remove(pod.getMetadata().getName());
    }


    @Override
    public List<Pod> getItems() {
        return new ArrayList<>(nameIndexMap.values());
    }
}
