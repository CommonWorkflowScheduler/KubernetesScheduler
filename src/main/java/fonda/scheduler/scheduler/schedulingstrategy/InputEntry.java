package fonda.scheduler.scheduler.schedulingstrategy;

import java.util.LinkedList;
import java.util.List;

public class InputEntry {

    public final String currentIP;
    public final String node;
    public final List<String> files;

    public InputEntry( String currentIP, String node, List<String> files ) {
        this.currentIP = currentIP;
        this.node = node;
        this.files = files;
    }
}
