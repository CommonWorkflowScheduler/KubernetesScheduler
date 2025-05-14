package cws.k8s.scheduler.model.outfiles;

import lombok.Getter;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

@Getter
public class SymlinkOutput extends OutputFile {

    private final Path dst;

    public SymlinkOutput( String path, String dst ) {
        super( Paths.get(path) );
        this.dst = Paths.get(dst);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SymlinkOutput that)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return Objects.equals(dst, that.dst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dst);
    }

    @Override
    public String toString() {
        return "SymlinkOutput{" + super.getPath() + " -> " + getDst() + "}";
    }
}
