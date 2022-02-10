package fonda.scheduler.model;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.outfiles.OutputFile;
import fonda.scheduler.model.outfiles.PathLocationWrapperPair;
import fonda.scheduler.model.outfiles.SymlinkOutput;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
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

    private String getRootDir( File file ){
        try ( Scanner sc  = new Scanner( file ) ) {
            if( sc.hasNext() ) return sc.next().split(";")[0];
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param workdir
     * @param location
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

        String taskRootDir = getRootDir( infile.toFile() );
        if( taskRootDir == null ) throw new IllegalStateException("taskRootDir is null");
        String outputRootDir = getRootDir( outfile.toFile() );
        if( outputRootDir == null ) throw new IllegalStateException("outputRootDir is null");

        final Map<String, String> inputdata = new HashMap<>();

        Set<OutputFile> newOrUpdated = new HashSet<>();

        try (
                Stream<String> in = Files.lines(infile);
                Stream<String> out = Files.lines(outfile)
        ) {

            in.skip( 1 )
                .forEach( line -> {
                    String[] data = line.split(";");
                    if( data[ FILE_EXISTS ].equals("0") && data.length != 8 ) return;
                    String path = data[ REAL_PATH ].equals("") ? data[ VIRTUAL_PATH ].substring( taskRootDir.length() + 1 ) : data[ REAL_PATH ];
                    String modificationDate = data[ MODIFICATION_DATE ];
                    inputdata.put( path , modificationDate );
                });

            log.trace( "{}", inputdata );

            out.skip( 1 )
                .forEach( line -> {
                    String[] data = line.split(";");
                    if( data[ FILE_EXISTS ].equals("0") && data.length != 8 ) return;
                    boolean realFile = data[ REAL_PATH ].equals("");
                    String path = realFile ? data[ VIRTUAL_PATH ] : data[ REAL_PATH ];
                    String modificationDate = data[ MODIFICATION_DATE ];
                    if ( "directory".equals(data[ FILE_TYPE ]) ) return;
                    String lockupPath = realFile ? path.substring( outputRootDir.length() + 1 ) : path;
                    if ( ( !inputdata.containsKey(lockupPath) && !onlyUpdated )
                            ||
                            !modificationDate.equals( inputdata.get( lockupPath ) ))
                    {
                            final LocationWrapper locationWrapper = new LocationWrapper(
                                                                            location,
                                                                            fileTimeFromString(modificationDate),
                                                                            Long.parseLong(data[ SIZE ]),
                                                                            finishedTask
                                                                    );
                            newOrUpdated.add( new PathLocationWrapperPair( Paths.get(path), locationWrapper ) );
                    }
                    if( !realFile ){
                        newOrUpdated.add( new SymlinkOutput( data[ VIRTUAL_PATH ], data[ REAL_PATH ]));
                    }
                });

        } catch (IOException e) {
            e.printStackTrace();
        }
        return newOrUpdated;

    }

    private long fileTimeFromString(String date) {
        if( date == null || date.equals("-")) {
            throw new RuntimeException("Date was: " + date);
        }
        try {
            if (Character.isLetter(date.charAt(0))) {
                // if ls was used, date has the format "Nov 2 08:49:30 2021"
                return new SimpleDateFormat("MMM dd HH:mm:ss yyyy").parse(date).getTime();
            } else {
                // if stat was used, date has the format "2021-11-02 08:49:30.955691861 +0000"
                String[] parts = date.split(" ");
                parts[1] = parts[1].substring(0, 12);
                // parts[1] now has milliseconds as smallest units e.g. "08:49:30.955"
                String shortenedDate = String.join(" ", parts);
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z").parse(shortenedDate).getTime();
            }
        } catch ( Exception e ){
            throw new RuntimeException( "Cannot parse date: " + date, e );
        }
    }

}
