package fonda.scheduler.model.outfiles;

import lombok.Getter;

import java.nio.file.Path;
import java.util.Objects;

@Getter
public class OutputFile {

    private final Path path;

    public OutputFile(Path path) {
        this.path = path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OutputFile)) {
            return false;
        }
        OutputFile that = (OutputFile) o;
        return Objects.equals(getPath(), that.getPath());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPath());
    }
}
