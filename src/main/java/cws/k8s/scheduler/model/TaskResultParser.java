package cws.k8s.scheduler.model;

import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.outfiles.OutputFile;
import cws.k8s.scheduler.model.outfiles.PathLocationWrapperPair;
import cws.k8s.scheduler.model.outfiles.SymlinkOutput;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
public class TaskResultParser {

    static final int VIRTUAL_PATH = 0;
    static final int FILE_EXISTS = 1;
    static final int REAL_PATH = 2;
    static final int SIZE = 3;
    static final int FILE_TYPE = 4;
    static final int CREATION_DATE = 5;
    static final int ACCESS_DATE = 6;
    static final int MODIFICATION_DATE = 7;

    public String getIndex( File file, int index ){
        try ( Scanner sc  = new Scanner( file ) ) {
            int i = 0;
            while( sc.hasNext() && i++ < index ) {
                sc.nextLine();
            }
            if ( sc.hasNext() ) {
                return sc.nextLine();
            }
        } catch (FileNotFoundException e) {
            log.error( "Cannot read " + file, e);
        }
        return null;
    }

    private String getRootDir( File file, int index ){
        String data = getIndex( file, index );
        if ( data == null ) {
            return null;
        }
        return data.split(";")[0];
    }

    private Long getDateDir(File file ){
        String data = getIndex( file, 0 );
        if ( data == null ) {
            return null;
        }
        return Long.parseLong( data );
    }

    public void processInput( Stream<String> in, final Set<String> inputdata ){
        in.skip( 2 )
                .forEach( line -> {
                    String[] data = line.split(";");
                    if( data[ FILE_EXISTS ].equals("0") ) {
                        return;
                    }
                    if ( data[ 3 ].equals("directory") ) {
                       return;
                    }
                    String path = data[ REAL_PATH ].equals("") ? data[ VIRTUAL_PATH ]  : data[ REAL_PATH ];
                    inputdata.add( path );
                });
    }

    private Set<OutputFile> processOutput(
            final Stream<String> out,
            final Set<String> inputdata,
            final Location location,
            final boolean onlyUpdated,
            final Task finishedTask,
            final String outputRootDir,
            final long initailDate
            ){
        final Set<OutputFile> newOrUpdated = new HashSet<>();
        out.skip( 1 )
                .forEach( line -> {
                    String[] data = line.split(";");
                    if( data[ FILE_EXISTS ].equals("0") && data.length != 8 ) {
                        return;
                    }
                    boolean isSymlink = !data[ REAL_PATH ].equals("");
                    String path = isSymlink ? data[ REAL_PATH ] : data[ VIRTUAL_PATH ];
                    String modificationDate = data[ MODIFICATION_DATE ];
                    if ( "directory".equals( data[ FILE_TYPE ] ) ) {
                        return;
                    }
                    String lockupPath = isSymlink ? path : path.substring( outputRootDir.length() );
                    long modificationDateNano = Long.parseLong( modificationDate );
                    if ( ( !inputdata.contains(lockupPath) && !onlyUpdated )
                            ||
                            modificationDateNano > initailDate )
                    {
                        final LocationWrapper locationWrapper = new LocationWrapper(
                                location,
                                modificationDateNano / (int) 1.0E6,
                                Long.parseLong(data[ SIZE ]),
                                finishedTask
                        );
                        newOrUpdated.add( new PathLocationWrapperPair( Paths.get(path), locationWrapper ) );
                    }
                    if( isSymlink ){
                        newOrUpdated.add( new SymlinkOutput( data[ VIRTUAL_PATH ], path ));
                    }
                });
        return newOrUpdated;
    }

    /**
     *
     * @param workdir
     * @param location
     * @param onlyUpdated
     * @param finishedTask
     * @return A list of all new or updated files
     */
    public Set<OutputFile> getNewAndUpdatedFiles(
            final Path workdir,
            final Location location,
            final boolean onlyUpdated,
            Task finishedTask
    ){

        final Path infile = workdir.resolve(".command.infiles");
        final Path outfile = workdir.resolve(".command.outfiles");
        if ( !outfile.toFile().exists() ) {
            log.error( "Cannot find outfile " + infile );
            return new HashSet<>();
        }

        final String taskRootDir = getRootDir( infile.toFile(), 1 );
        if( taskRootDir == null
                && (finishedTask.getInputFiles() == null || finishedTask.getInputFiles().isEmpty()) ) {
            throw new IllegalStateException("taskRootDir is null");
        }


        final String outputRootDir = getRootDir( outfile.toFile(), 0 );
        //No outputs defined / found
        if( outputRootDir == null ) {
            return new HashSet<>();
        }

        final Set<String> inputdata = new HashSet<>();


        try (
                Stream<String> in = Files.lines(infile);
                Stream<String> out = Files.lines(outfile)
        ) {

            processInput( in, inputdata );
            log.trace( "{}", inputdata );
            final Long initialDate = getDateDir( infile.toFile() );
            return processOutput( out, inputdata, location, onlyUpdated, finishedTask, outputRootDir, initialDate );

        } catch (IOException e) {
            log.error( "Cannot read in/outfile in workdir: " + workdir, e);
        }
        return new HashSet<>();

    }

}
