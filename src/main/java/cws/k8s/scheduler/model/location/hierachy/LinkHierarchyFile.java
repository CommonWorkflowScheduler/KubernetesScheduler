package cws.k8s.scheduler.model.location.hierachy;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.nio.file.Path;

@RequiredArgsConstructor
public class LinkHierarchyFile extends AbstractHierarchyFile {

    @Getter
    private final Path dst;

    @Override
    public boolean isDirectory() {
        throw new IllegalStateException("Call on link");
    }

    @Override
    public boolean isSymlink() {
        return true;
    }
}
