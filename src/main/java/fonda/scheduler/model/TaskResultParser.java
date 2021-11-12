package fonda.scheduler.model;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class TaskResultParser {

    private String getRootDir( File file ){
        try {
            Scanner sc = new Scanner( file );
            if( sc.hasNext() ) return sc.next().split(";")[0];
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param workdir
     * @return A list of all new or updated files
     */
    public Set<PathLocationWrapperPair> getNewAndUpdatedFiles(final Path workdir, Location location, Process process ){

        final Path infile = workdir.resolve(".command.infiles");
        final Path outfile = workdir.resolve(".command.outfiles");

        String taskRootDir = getRootDir( infile.toFile() );
        String outputRootDir = getRootDir( outfile.toFile() );

        final Map<String, String> inputdata = new HashMap<>();

        Set<PathLocationWrapperPair> newOrUpdated = new HashSet<>();

        try {
            Files.lines( infile ).skip( 1 )
                .forEach( line -> {
                    String[] data = line.split(";");
                    String path = data[1].equals("") ? data[0].substring( taskRootDir.length() + 1 ) : data[1];
                    String modificationDate = data[6];
                    inputdata.put( path , modificationDate );
                });

            log.info( "{}", inputdata );

            Files.lines( outfile ).skip( 1 )
                .forEach( line -> {
                    String[] data = line.split(";");
                    boolean realFile = data[1].equals("");
                    String path = realFile ? data[0] : data[1];
                    String modificationDate = data[6];
                    if ( "directory".equals(data[3]) ) return;
                    String lockupPath = realFile ? path.substring( outputRootDir.length() + 1 ) : path;
                    if ( !inputdata.containsKey(lockupPath)
                            ||
                            !modificationDate.equals( inputdata.get( lockupPath ) ))
                    {
                            final LocationWrapper locationWrapper = new LocationWrapper(
                                                                            location,
                                                                            fileTimeFromString(modificationDate),
                                                                            Long.parseLong(data[2]),
                                                                            process
                                                                    );
                            newOrUpdated.add( new PathLocationWrapperPair( Paths.get(path), locationWrapper ) );
                    }
                });

        } catch (IOException e) {
            e.printStackTrace();
        }
        return newOrUpdated;

    }

    private long fileTimeFromString(String date) {
        if( date == null || date == "-" ) {
            return -1;
        }
        String[] parts = date.split(" ");
        parts[1] = parts[1].substring(0, 12);
        String shortenedDate = String.join(" ", parts);
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z").parse(shortenedDate).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return -1;
    }

}
