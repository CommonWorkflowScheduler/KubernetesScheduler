package fonda.scheduler.model;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.HashMap;
import java.util.Map;

public class KubernetesClientHolder {

    private final Map<String, KubernetesClient> holder = new HashMap<>();

    public KubernetesClient getClient(String namespace ){

        synchronized ( holder ){
            if( !holder.containsKey( namespace.toLowerCase() ) ){
                DefaultKubernetesClient kubernetesClient = new DefaultKubernetesClient();
                kubernetesClient.getConfiguration().setNamespace(namespace);
                holder.put( namespace.toLowerCase(), kubernetesClient );
            }
            return holder.get( namespace.toLowerCase() );
        }

    }

}
