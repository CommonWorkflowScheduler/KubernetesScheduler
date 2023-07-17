package cws.k8s.scheduler.model;

import java.math.BigDecimal;

public class ImmutableRequirements extends Requirements {

    public static final Requirements ZERO = new ImmutableRequirements();

    private static final String ERROR_MESSAGE = "ImmutableRequirements cannot be modified";

    public ImmutableRequirements( BigDecimal cpu, BigDecimal ram ) {
        super( cpu, ram );
    }

    public ImmutableRequirements(){
        super();
    }

    public ImmutableRequirements( Requirements requirements ){
        super( requirements.getCpu(), requirements.getRam() );
    }

    @Override
    public Requirements addCPUtoThis( BigDecimal cpu ) {
        throw new IllegalStateException( ERROR_MESSAGE );
    }

    @Override
    public Requirements addRAMtoThis( BigDecimal ram ) {
        throw new IllegalStateException( ERROR_MESSAGE );
    }

    @Override
    public Requirements addToThis( Requirements requirements ) {
        throw new IllegalStateException( ERROR_MESSAGE );
    }

    @Override
    public Requirements subFromThis( Requirements requirements ) {
        throw new IllegalStateException( ERROR_MESSAGE );
    }
}
