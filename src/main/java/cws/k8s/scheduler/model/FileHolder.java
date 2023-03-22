package cws.k8s.scheduler.model;

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

    /**
     * Only for testing
     * @param storePath
     * @param sourceObj
     * @param stageName
     */
    public FileHolder( String storePath, String sourceObj, String stageName ) {
        this.storePath = storePath;
        this.sourceObj = sourceObj;
        this.stageName = stageName;
    }

}
