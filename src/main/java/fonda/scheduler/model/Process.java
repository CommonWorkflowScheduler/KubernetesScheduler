package fonda.scheduler.model;

import lombok.Getter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Process {

    static final private ConcurrentMap< String, Process > processHolder = new ConcurrentHashMap<String, Process>();

    @Getter
    private final String name;

    private Process(String name) {
        this.name = name;
    }

    public static Process getProcess ( final String process ){
        final Process processFound = processHolder.get( process );
        if ( processFound == null ){
            processHolder.putIfAbsent( process, new Process( process ) );
            return processHolder.get( process );
        }
        return processFound;
    }

}
