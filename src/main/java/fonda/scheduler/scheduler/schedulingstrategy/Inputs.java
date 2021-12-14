package fonda.scheduler.scheduler.schedulingstrategy;

import fonda.scheduler.util.inputs.SymlinkInput;

import java.util.LinkedList;
import java.util.List;

public class Inputs {

    public final String dns;
    public final List<InputEntry> data;
    public final List<SymlinkInput> symlinks;

    public Inputs( String dns ) {
        this.dns = dns;
        this.data = new LinkedList<>();
        this.symlinks = new LinkedList<>();
    }

}
