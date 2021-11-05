package fonda.scheduler.model.location;

import java.util.Objects;

public abstract class Location {

    abstract String getIdentifier();

    abstract LocationType getType();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof Location) ) return false;
        Location that = (Location) o;
        return ( getType() == that.getType() ) && ( getIdentifier().equals( that.getIdentifier() ));
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIdentifier(),getType());
    }


}
