package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString( exclude = {"stageName", "storePath"})
@NoArgsConstructor(access = AccessLevel.NONE)
public class FileHolder {

    public final String storePath;
    public final String sourceObj;
    public final String stageName;

    /**
     * Only for testing
     */
    public FileHolder( String storePath, String sourceObj, String stageName ) {
        this.storePath = storePath;
        this.sourceObj = sourceObj;
        this.stageName = stageName;
    }

}
