package cws.k8s.scheduler.config;

import cws.k8s.scheduler.client.KubernetesClient;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor( access = AccessLevel.PACKAGE )
public class Beans {

    private static KubernetesClient kubernetesClient;

    @Bean
    KubernetesClient getClient(){
        if(kubernetesClient == null) {
            kubernetesClient = new KubernetesClient();
            kubernetesClient.getConfiguration().setNamespace(null);
        }
        return kubernetesClient;
    }

}
