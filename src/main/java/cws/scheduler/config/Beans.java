package fonda.scheduler.config;

import fonda.scheduler.client.KubernetesClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Beans {

    private static KubernetesClient kubernetesClient;

    Beans() {}

    @Bean
    KubernetesClient getClient(){
        if(kubernetesClient == null) {
            kubernetesClient = new KubernetesClient();
            kubernetesClient.getConfiguration().setNamespace(null);
        }
        return kubernetesClient;
    }

}
