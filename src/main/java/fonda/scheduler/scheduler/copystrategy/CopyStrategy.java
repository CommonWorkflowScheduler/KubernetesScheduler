package fonda.scheduler.scheduler.copystrategy;

import fonda.scheduler.model.Task;

import java.io.*;
import java.nio.charset.StandardCharsets;

public abstract class CopyStrategy {

    public void generateCopyScript( Task task ){

        File file = new File(task.getWorkingDir() + '/' + ".command.init.run");

        try (PrintWriter pw = new PrintWriter(file)) {

            ClassLoader classLoader = getClass().getClassLoader();

            try (InputStream inputStream = classLoader.getResourceAsStream( getResource() );
                 InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(streamReader)) {

                String line;
                while ((line = reader.readLine()) != null) {
                    pw.println(line);
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
