package fonda.scheduler.scheduler.schedulingstrategy;

import fonda.scheduler.model.Task;
import fonda.scheduler.model.taskinputs.SymlinkInput;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Inputs {

    public final String dns;
    public final String hash;
    public final List<InputEntry> data;
    public final List<SymlinkInput> symlinks;
    public final String syncDir;
    public final Map<String,List<String>> waitForFilesOfTask;

    public Inputs( String dns, String syncDir, String hash ) {
        this.dns = dns;
        this.syncDir = syncDir;
        this.hash = hash;
        this.data = new LinkedList<>();
        this.symlinks = new LinkedList<>();
        waitForFilesOfTask = new ConcurrentHashMap<>();
    }

    public void waitForTask( Map<String, Task> waitForTask ){
        for (Map.Entry<String, Task> e : waitForTask.entrySet()) {
            final String hash = e.getValue().getConfig().getHash();
            if ( !waitForFilesOfTask.containsKey( hash ) ) waitForFilesOfTask.put( hash, new LinkedList<>() );
            final List<String> listOfPaths = waitForFilesOfTask.get(hash);
            listOfPaths.add( e.getKey() );
        }
    }

}
