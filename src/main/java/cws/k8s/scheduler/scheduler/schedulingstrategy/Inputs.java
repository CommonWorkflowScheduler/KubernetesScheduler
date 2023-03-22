package cws.k8s.scheduler.scheduler.schedulingstrategy;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.taskinputs.SymlinkInput;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
public class Inputs {

    public final String dns;
    public final String execution;
    public final List<InputEntry> data = new LinkedList<>();
    public final List<SymlinkInput> symlinks = new LinkedList<>();
    public final String syncDir;
    public final String hash;
    public final Map<String,List<String>> waitForFilesOfTask = new ConcurrentHashMap<>();
    public final int speed;

    public void waitForTask( Map<String, Task> waitForTask ){
        for (Map.Entry<String, Task> e : waitForTask.entrySet()) {
            final String taskHash = e.getValue().getConfig().getRunName();
            final List<String> listOfPaths = waitForFilesOfTask.computeIfAbsent( taskHash, k -> new LinkedList<>() );
            listOfPaths.add( e.getKey() );
        }
    }

    public void sortData(){
        data.sort(Collections.reverseOrder());
    }

}
