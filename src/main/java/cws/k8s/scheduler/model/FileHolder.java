package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString( exclude = {"stageName", "storePath"})
@NoArgsConstructor(access = AccessLevel.NONE, force = true)
@RequiredArgsConstructor
public class FileHolder {

    public final String storePath;
    public final String sourceObj;
    public final String stageName;

}
