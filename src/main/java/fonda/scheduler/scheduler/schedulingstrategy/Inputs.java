package fonda.scheduler.scheduler.schedulingstrategy;

import java.util.LinkedList;
import java.util.List;

public class Inputs {

    public final String dns;
    public final List<InputEntry> data;

    public Inputs( String dns ) {
        this.dns = dns;
        this.data = new LinkedList<>();
    }

}
