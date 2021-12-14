package fonda.scheduler.model.location.hierachy;

import lombok.Getter;

import java.nio.file.Path;

public class LinkFile extends AbstractFile {

    @Getter
    private final Path dst;

    public LinkFile( Path dst ) {
        this.dst = dst;
    }

    @Override
    public boolean isDirectory() {
        throw new IllegalStateException("Call on link");
    }

    @Override
    public boolean isSymlink() {
        return true;
    }
}
