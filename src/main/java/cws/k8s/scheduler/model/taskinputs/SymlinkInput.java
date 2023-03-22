package cws.k8s.scheduler.model.taskinputs;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.nio.file.Path;

@Getter
@EqualsAndHashCode
public class SymlinkInput implements Input {

    private final String src;
    private final String dst;

    public SymlinkInput(Path src, Path dst) {
        this.src = src.toAbsolutePath().toString();
        this.dst = dst.toAbsolutePath().toString();
    }

}
