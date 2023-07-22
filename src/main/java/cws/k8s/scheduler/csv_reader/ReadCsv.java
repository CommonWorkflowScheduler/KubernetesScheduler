package cws.k8s.scheduler.csv_reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


import org.apache.commons.lang3.tuple.Pair;

import lombok.Getter;

public class ReadCsv {

    @Getter
    private final String pathToCsv;

    @Getter
    private final Map<String, Pair<String, Integer>> labelNameToNodeResource = new HashMap<>();

    public ReadCsv(String pathToCsv) {
        this.pathToCsv = pathToCsv;
    }

    public void readAndProcessCsv(String[] args) {
        boolean isHeader = true; // Flag to check if there is a header

        try (BufferedReader br = new BufferedReader(new FileReader(pathToCsv))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue; // Skip the header
                }
                String[] columns = line.split(";");
                if (columns.length == 3) {
                    String nodeName = columns[0];
                    String labelName = columns[1];
                    int resource = Integer.parseInt(columns[2]);


                    Pair<String, Integer> nodeResourcePair = Pair.of(nodeName, resource);
                    labelNameToNodeResource.put(labelName, nodeResourcePair);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for( Map.Entry<String, Pair<String, Integer>> e : labelNameToNodeResource.entrySet()) {
            System.out.println(e.getKey() + "/" + e.getValue().getLeft() + "/" + e.getValue().getRight());
        }
    }
}