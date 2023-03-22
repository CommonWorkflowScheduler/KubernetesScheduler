package cws.k8s.scheduler.model.outfiles;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.nio.file.Path;
import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class OutputFile {

    private final Path path;

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
