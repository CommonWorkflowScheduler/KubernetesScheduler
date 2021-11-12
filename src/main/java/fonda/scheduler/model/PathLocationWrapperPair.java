package fonda.scheduler.model;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import lombok.Getter;

import java.nio.file.Path;
import java.util.Objects;

@Getter
public class PathLocationWrapperPair {

    private final Path path;
    private final LocationWrapper locationWrapper;

    public PathLocationWrapperPair(Path path, LocationWrapper locationWrapper) {
        this.path = path;
        this.locationWrapper = locationWrapper;
    }

    @Override
    public String toString() {
        return "PathLocationWrapperPair{" +
                "path=" + path +
                ", locationWrapper=" + locationWrapper +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PathLocationWrapperPair)) return false;
        PathLocationWrapperPair that = (PathLocationWrapperPair) o;
        return getPath().equals(that.getPath()) && getLocationWrapper().equals(that.getLocationWrapper());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPath(), getLocationWrapper());
    }
}
