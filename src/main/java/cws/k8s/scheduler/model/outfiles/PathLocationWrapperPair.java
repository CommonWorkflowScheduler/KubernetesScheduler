package cws.k8s.scheduler.model.outfiles;

import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import lombok.Getter;

import java.nio.file.Path;
import java.util.Objects;

@Getter
public class PathLocationWrapperPair extends OutputFile {

    private final LocationWrapper locationWrapper;

    public PathLocationWrapperPair(Path path, LocationWrapper locationWrapper) {
        super( path );
        this.locationWrapper = locationWrapper;
    }

    @Override
    public String toString() {
        return "PathLocationWrapperPair{" +
                "path=" + getPath() +
                ", locationWrapper=" + locationWrapper +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PathLocationWrapperPair)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PathLocationWrapperPair that = (PathLocationWrapperPair) o;
        return Objects.equals(getLocationWrapper(), that.getLocationWrapper());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getLocationWrapper());
    }

}
