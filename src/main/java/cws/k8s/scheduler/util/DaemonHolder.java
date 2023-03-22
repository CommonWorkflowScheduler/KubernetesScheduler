package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.location.NodeLocation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DaemonHolder {

    final Map<String, DaemonData> daemonByNode = new HashMap<>();

    public void removeDaemon(String nodeName) {
        daemonByNode.remove(nodeName);
    }

    public void addDaemon(String node, String daemonName, String daemonIp ) {
        daemonByNode.put(node, new DaemonData( daemonName, daemonIp ) );
    }

    public String getDaemonIp(String node) {
        final DaemonData daemonData = daemonByNode.get( node );
        return daemonData == null ? null : daemonData.getIp();
    }

    public String getDaemonName(String node) {
        final DaemonData daemonData = daemonByNode.get( node );
        return daemonData == null ? null : daemonData.getName();
    }

    public String getDaemonIp( NodeLocation node) {
        return getDaemonIp(node.getIdentifier());
    }

    public String getDaemonName(NodeLocation node) {
        return getDaemonName(node.getIdentifier());
    }

    @Getter
    @ToString
    @AllArgsConstructor
    private class DaemonData {
        private String name;
        private String ip;
    }

    @Override
    public String toString() {
        return daemonByNode.toString();
    }
}
