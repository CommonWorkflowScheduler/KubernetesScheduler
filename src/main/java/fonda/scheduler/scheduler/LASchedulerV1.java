package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
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
