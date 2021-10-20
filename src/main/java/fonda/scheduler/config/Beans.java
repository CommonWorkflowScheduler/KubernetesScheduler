package fonda.scheduler.config;

import fonda.scheduler.model.KubernetesClientHolder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Beans {

    private static KubernetesClientHolder kubernetesClientHolder;

    @Bean
    KubernetesClientHolder getClientHolder(){
        if(kubernetesClientHolder == null) {
            kubernetesClientHolder = new KubernetesClientHolder();
        }
        return kubernetesClientHolder;
    }

}
