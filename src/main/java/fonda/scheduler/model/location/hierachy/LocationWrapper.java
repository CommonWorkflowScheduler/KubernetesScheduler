package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.location.Location;
import lombok.Getter;

@Getter
public class LocationWrapper {

    private final Location location;
    private final long timestamp;
    private final long sizeInBytes;
    private final String process;

    public LocationWrapper(Location location, long timestamp, long sizeInBytes, String process) {
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
}
