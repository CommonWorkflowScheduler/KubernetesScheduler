package fonda.scheduler.model;

import lombok.Getter;

import java.math.BigDecimal;

public class PodRequirements {

    public static final PodRequirements ZERO = new PodRequirements();

    @Getter
    private BigDecimal cpu;
    @Getter
    private BigDecimal ram;

    public PodRequirements( BigDecimal cpu, BigDecimal ram ) {
        this.cpu = cpu == null ? BigDecimal.ZERO : cpu;
        this.ram = ram == null ? BigDecimal.ZERO : ram;
    }

    public PodRequirements(){
        this( BigDecimal.ZERO, BigDecimal.ZERO );
    }

    public PodRequirements addToThis( PodRequirements podRequirements ){
        this.cpu = this.cpu.add(podRequirements.cpu);
        this.ram = this.ram.add(podRequirements.ram);
        return this;
    }

    public PodRequirements addRAMtoThis( BigDecimal ram ){
        this.ram = this.ram.add( ram );
        return this;
    }

    public PodRequirements addCPUtoThis( BigDecimal cpu ){
        this.cpu = this.cpu.add( cpu );
        return this;
    }

    public PodRequirements subFromThis( PodRequirements podRequirements ){
        this.cpu = this.cpu.subtract(podRequirements.getCpu());
        this.ram = this.ram.subtract(podRequirements.getRam());
        return this;
    }

    public PodRequirements sub( PodRequirements podRequirements ){
        return new PodRequirements(
                this.cpu.subtract(podRequirements.getCpu()),
                this.ram.subtract(podRequirements.getRam())
        );
    }

    public boolean higherOrEquals( PodRequirements podRequirements ){
        return this.cpu.compareTo( podRequirements.cpu ) >= 0
                && this.ram.compareTo( podRequirements.ram ) >= 0;
    }

    @Override
    public String toString() {
        return "PodRequirements{" +
                "cpu=" + cpu +
                ", ram=" + ram +
                '}';
    }
}
