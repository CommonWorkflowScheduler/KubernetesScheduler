package cws.k8s.scheduler.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.math.BigDecimal;

import static cws.k8s.scheduler.util.Formater.formatBytes;

@ToString
@EqualsAndHashCode
public class Requirements implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public static final Requirements ZERO = new Requirements();

    @Getter
    private BigDecimal cpu;
    @Getter
    private BigDecimal ram;

    public Requirements( BigDecimal cpu, BigDecimal ram ) {
        this.cpu = cpu == null ? BigDecimal.ZERO : cpu;
        this.ram = ram == null ? BigDecimal.ZERO : ram;
    }

    /**
     * Basically used for testing
     * @param cpu
     * @param ram
     */
    public Requirements( int cpu, int ram ) {
        this.cpu = BigDecimal.valueOf(cpu);
        this.ram = BigDecimal.valueOf(ram);
    }

    public Requirements(){
        this( BigDecimal.ZERO, BigDecimal.ZERO );
    }

    public Requirements addToThis( Requirements requirements ){
        this.cpu = this.cpu.add(requirements.cpu);
        this.ram = this.ram.add(requirements.ram);
        return this;
    }

    public Requirements add( Requirements requirements ){
        return new Requirements(
                this.cpu.add(requirements.cpu),
                this.ram.add(requirements.ram)
        );
    }

    public Requirements addRAMtoThis( BigDecimal ram ){
        this.ram = this.ram.add( ram );
        return this;
    }

    public Requirements addCPUtoThis( BigDecimal cpu ){
        this.cpu = this.cpu.add( cpu );
        return this;
    }

    public Requirements subFromThis( Requirements requirements ){
        this.cpu = this.cpu.subtract(requirements.getCpu());
        this.ram = this.ram.subtract(requirements.getRam());
        return this;
    }

    public Requirements sub( Requirements requirements ){
        return new Requirements(
                this.cpu.subtract(requirements.getCpu()),
                this.ram.subtract(requirements.getRam())
        );
    }

    public Requirements multiply( BigDecimal factor ){
        return new Requirements(
                this.cpu.multiply(factor),
                this.ram.multiply(factor)
        );
    }

    public Requirements multiplyToThis( BigDecimal factor ){
        this.cpu = this.cpu.multiply(factor);
        this.ram = this.ram.multiply(factor);
        return this;
    }

    public boolean higherOrEquals( Requirements requirements ){
        return this.cpu.compareTo( requirements.cpu ) >= 0
                && this.ram.compareTo( requirements.ram ) >= 0;
    }

    @Override
    public String toString() {
        return "Requirements{" +
                "cpu=" + cpu +
                ", ram=" + formatBytes( ram.longValue() )  +
                '}';
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) return true;
        if ( !(o instanceof Requirements that) ) return false;

        if ( getCpu() != null ? !getCpu().equals( that.getCpu() ) : that.getCpu() != null ) return false;
        return getRam() != null ? getRam().equals( that.getRam() ) : that.getRam() == null;
    }

    @Override
    public int hashCode() {
        int result = getCpu() != null ? getCpu().hashCode() : 0;
        result = 31 * result + (getRam() != null ? getRam().hashCode() : 0);
        return result;
    }

    /**
     * Always returns a mutable copy of this object
     * @return
     */
    @Override
    public Requirements clone() {
        return new Requirements( this.cpu, this.ram );
    }

    public boolean smaller( Requirements request ) {
        return this.cpu.compareTo( request.cpu ) < 0
                && this.ram.compareTo( request.ram ) < 0;
    }

    public boolean smallerEquals( Requirements request ) {
        return this.cpu.compareTo( request.cpu ) <= 0
                && this.ram.compareTo( request.ram ) <= 0;
    }

    public boolean atLeastOneBigger( Requirements request ) {
        return this.cpu.compareTo( request.cpu ) > 0
                || this.ram.compareTo( request.ram ) > 0;
    }

}
