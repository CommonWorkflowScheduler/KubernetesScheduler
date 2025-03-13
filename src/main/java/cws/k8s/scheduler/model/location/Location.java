package cws.k8s.scheduler.model.location;

import java.io.Serializable;
import java.util.Objects;

public abstract class Location implements Serializable {

    public abstract String getIdentifier();

    public abstract LocationType getType();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Location)) {
            return false;
        }
        Location that = (Location) o;
        return ( getType() == that.getType() ) && ( getIdentifier().equals( that.getIdentifier() ));
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIdentifier(),getType());
    }


}
