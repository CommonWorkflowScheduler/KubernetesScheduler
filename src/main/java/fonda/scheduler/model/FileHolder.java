package fonda.scheduler.model;

import lombok.ToString;

@ToString( exclude = {"stageName", "storePath"})
public class FileHolder {

    public final String storePath;
    public final String sourceObj;
    public final String stageName;

    private FileHolder() {
        this.storePath = null;
        this.sourceObj = null;
        this.stageName = null;
    }

}
