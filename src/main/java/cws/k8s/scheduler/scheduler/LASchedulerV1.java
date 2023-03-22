package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LASchedulerV1 extends LocationAwareScheduler {

    public LASchedulerV1(
            String name,
            KubernetesClient client,
            String namespace,
            SchedulerConfig config,
            InputAlignment inputAlignment
    ) {
        super(name, client, namespace, config, inputAlignment);
    }

}
