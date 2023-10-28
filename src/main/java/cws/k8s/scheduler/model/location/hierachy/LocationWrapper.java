package cws.k8s.scheduler.model.location.hierachy;

import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.Task;
import lombok.Getter;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Getter
public class LocationWrapper {

    private static final AtomicLong nextID = new AtomicLong(0);

    private final long id = nextID.getAndIncrement();
    private final Location location;
    private long timestamp;
    private long sizeInBytes;
    private long createTime = System.currentTimeMillis();
    private Task createdByTask;
    private LocationWrapper copyOf;
    //Deactivated if file was maybe not copied completely or if one file was changed by the workflow engine.
    private boolean active = true;
    private int inUse = 0;

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

    public void update( LocationWrapper update ){
        if (location != update.location) {
            throw new IllegalArgumentException( "Can only update LocationWrapper with the same location." );
        }
        synchronized ( this ) {
            this.timestamp = update.timestamp;
            this.sizeInBytes = update.sizeInBytes;
            this.createTime = update.createTime;
            this.createdByTask = update.createdByTask;
            this.copyOf = update.copyOf;
            this.active = update.active;
        }
    }

    public void deactivate(){
        this.active = false;
    }

    /**
     * use the file, if you copy it to a node, or a task uses it as input
     */
    public void use(){
        synchronized ( this ) {
            inUse++;
        }
    }

    /**
     * free the file, if you finished copy it to a node, or a task the task finished that used it as an input
     */
    public void free(){
        synchronized ( this ) {
            inUse--;
        }
    }

    /**
     * Any task currently reading or writing to this file
     */
    public boolean isInUse(){
        return inUse > 0;
    }

    public LocationWrapper getCopyOf( Location location ) {
        synchronized ( this ) {
            return new LocationWrapper(location, timestamp, sizeInBytes, createdByTask, copyOf == null ? this : copyOf);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LocationWrapper)) {
            return false;
        }

        LocationWrapper that = (LocationWrapper) o;

        synchronized ( this ) {
            if (getTimestamp() != that.getTimestamp()) {
                return false;
            }
            if (getSizeInBytes() != that.getSizeInBytes()) {
                return false;
            }
            if (!getLocation().equals(that.getLocation())) {
                return false;
            }
            if (getCreatedByTask() != null ? !getCreatedByTask().equals(that.getCreatedByTask()) : that.getCreatedByTask() != null) {
                return false;
            }
            return getCopyOf() != null ? getCopyOf().equals(that.getCopyOf()) : that.getCopyOf() == null;
        }
    }

    @Override
    public int hashCode() {
        synchronized ( this ) {
            return Objects.hash(getLocation(), getTimestamp(), getSizeInBytes(), getCreatedByTask());
        }
    }

    @Override
    public String toString() {
        synchronized ( this ) {
            return "LocationWrapper{" +
                    "id=" + id +
                    ", active=" + active +
                    ", location=" + location.getIdentifier() +
                    ", timestamp=" + timestamp +
                    ", inUse=" + inUse + "x" +
                    ", sizeInBytes=" + sizeInBytes +
                    ", createTime=" + createTime +
                    ", createdByTask=" + createdByTask +
                    ", copyOf=" + copyOf +
                    '}';
        }
    }
}
