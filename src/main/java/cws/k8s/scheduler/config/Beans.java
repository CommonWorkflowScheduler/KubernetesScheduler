package cws.k8s.scheduler.config;

import cws.k8s.scheduler.client.CWSKubernetesClient;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor( access = AccessLevel.PACKAGE )
public class Beans {

    private static CWSKubernetesClient kubernetesClient;

    @Bean
    CWSKubernetesClient getClient(){
        if(kubernetesClient == null) {
            kubernetesClient = new CWSKubernetesClient();
            kubernetesClient.getConfiguration().setNamespace(null);
        }
        return kubernetesClient;
    }

}
