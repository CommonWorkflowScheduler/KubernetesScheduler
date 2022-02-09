package fonda.scheduler.scheduler.copystrategy;

import fonda.scheduler.model.Task;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public abstract class CopyStrategy {

    public void generateCopyScript( Task task ){

        File file = new File(task.getWorkingDir() + '/' + ".command.init.run");

        try (PrintWriter pw = new PrintWriter(file)) {

            ClassLoader classLoader = getClass().getClassLoader();

            try (InputStream inputStream = classLoader.getResourceAsStream( getResource() )) {
                assert inputStream != null;
                try (InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                     BufferedReader reader = new BufferedReader(streamReader)) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        pw.println(line);
                    }

                    Set<PosixFilePermission> executable = PosixFilePermissions.fromString("rwxrwxrwx");
                    Files.setPosixFilePermissions( file.toPath(), executable );
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    abstract String getResource();

}
