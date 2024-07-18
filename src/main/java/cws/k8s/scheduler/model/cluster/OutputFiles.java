package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.outfiles.PathLocationWrapperPair;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
@Getter
@RequiredArgsConstructor
public class OutputFiles {


    private final Set<PathLocationWrapperPair> files;
    private boolean wasRequestedForRealTask = false;

    public void wasRequestedForRealTask(){
        wasRequestedForRealTask = true;
    }

}
