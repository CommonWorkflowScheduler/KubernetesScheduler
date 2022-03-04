package fonda.scheduler.util.inputs;

import lombok.Getter;

import java.nio.file.Path;

@Getter
public class SymlinkInput implements Input {

    private final String src;
    private final String dst;

    public SymlinkInput(Path src, Path dst) {
        this.src = src.toAbsolutePath().toString();
        this.dst = dst.toAbsolutePath().toString();
    }
}
