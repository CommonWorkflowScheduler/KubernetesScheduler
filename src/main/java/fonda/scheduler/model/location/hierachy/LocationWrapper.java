package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.Process;
import fonda.scheduler.model.location.Location;
import lombok.Getter;

import java.util.Objects;

@Getter
public class LocationWrapper {

    private final Location location;
    private final long timestamp;
    private final long sizeInBytes;
    private final Process process;
    private final long createTime = System.currentTimeMillis();

    public LocationWrapper(Location location, long timestamp, long sizeInBytes, Process process) {
        this.location = location;
        this.timestamp = timestamp;
        this.sizeInBytes = sizeInBytes;
        this.process = process;
    }

    @Override
    public String toString() {
        return "LocationWrapper{" +
                "location=" + location +
                ", timestamp=" + timestamp +
                ", sizeInBytes=" + sizeInBytes +
                ", process='" + process + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LocationWrapper)) return false;
        LocationWrapper that = (LocationWrapper) o;
        return getTimestamp() == that.getTimestamp()
                && getSizeInBytes() == that.getSizeInBytes()
                && Objects.equals(getLocation(), that.getLocation())
                && Objects.equals(getProcess(), that.getProcess());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLocation(), getTimestamp(), getSizeInBytes(), getProcess());
    }
}
