package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import lombok.Getter;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public class LocationWrapper {

    private static final AtomicLong nextID = new AtomicLong(0);

    private final long id = nextID.getAndIncrement();
    private final Location location;
    private final long timestamp;
    private final long sizeInBytes;
    private final long createTime = System.currentTimeMillis();
    private final Task createdByTask;
    private final LocationWrapper copyOf;

    public LocationWrapper(Location location, long timestamp, long sizeInBytes) {
        this( location, timestamp, sizeInBytes ,null);
    }

    public LocationWrapper(Location location, long timestamp, long sizeInBytes, Task task) {
        this( location, timestamp, sizeInBytes ,task, null );
    }

    private LocationWrapper(Location location, long timestamp, long sizeInBytes, Task createdByTask, LocationWrapper copyOf) {
        this.location = location;
        this.timestamp = timestamp;
        this.sizeInBytes = sizeInBytes;
        this.createdByTask = createdByTask;
        this.copyOf = copyOf;
    }

    public LocationWrapper getCopyOf( Location location ) {
        return new LocationWrapper( location, timestamp, sizeInBytes, createdByTask, copyOf == null ? this : copyOf );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LocationWrapper)) return false;

        LocationWrapper that = (LocationWrapper) o;

        if (getTimestamp() != that.getTimestamp()) return false;
        if (getSizeInBytes() != that.getSizeInBytes()) return false;
        if (!getLocation().equals(that.getLocation())) return false;
        if (getCreatedByTask() != null ? !getCreatedByTask().equals(that.getCreatedByTask()) : that.getCreatedByTask() != null)
            return false;
        return getCopyOf() != null ? getCopyOf().equals(that.getCopyOf()) : that.getCopyOf() == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLocation(), getTimestamp(), getSizeInBytes(), getCreatedByTask());
    }

    @Override
    public String toString() {
        return "LocationWrapper{" +
                "id=" + id +
                ", location=" + location.getIdentifier() +
                ", timestamp=" + timestamp +
                ", sizeInBytes=" + sizeInBytes +
                ", createTime=" + createTime +
                ", createdByTask=" + createdByTask +
                ", copyOf=" + copyOf +
                '}';
    }
}
