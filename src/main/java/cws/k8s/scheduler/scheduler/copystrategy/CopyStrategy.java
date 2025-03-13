package cws.k8s.scheduler.scheduler.copystrategy;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

@Slf4j
public abstract class CopyStrategy {

    private void write( BufferedWriter pw, File file, String resource ) {
        ClassLoader classLoader = getClass().getClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream( resource )) {
            assert inputStream != null;
            try (InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(streamReader)) {

                String line;
                while ((line = reader.readLine()) != null) {
                    pw.write( line );
                    pw.write( '\n' );
                }

                Set<PosixFilePermission> executable = PosixFilePermissions.fromString("rwxrwxrwx");
                Files.setPosixFilePermissions( file.toPath(), executable );
            }
        } catch (IOException e) {
            log.error( "Cannot write " + file, e);
        }
    }

    abstract String getResource();

}
