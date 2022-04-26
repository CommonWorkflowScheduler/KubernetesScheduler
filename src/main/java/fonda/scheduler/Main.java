package fonda.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.rest.SchedulerRestController;
import fonda.scheduler.scheduler.RandomLAScheduler;
import fonda.scheduler.scheduler.filealignment.RandomAlignment;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
@Slf4j
public class Main {

    private final KubernetesClient client;

    public static void main(String[] args) {
        if( System.getenv( "SCHEDULER_NAME" ) == null || System.getenv( "SCHEDULER_NAME" ).isEmpty() ){
            throw new IllegalArgumentException( "Please define environment variable: SCHEDULER_NAME" );
        }
        SpringApplication.run(Main.class, args);
    }

    Main(@Autowired KubernetesClient client){
        this.client = client;
    }

    @PostConstruct
    public void afterStart(){
        try{
            log.info( "Started with namespace: {}", client.getNamespace() );
            final SchedulerConfig schedulerConfig = new SchedulerConfig(null, null, "/localwork/", null, "ftp", true);
            final RandomLAScheduler randomLAScheduler = new RandomLAScheduler("testscheduler", client, "default", schedulerConfig, new RandomAlignment());
            final Pair<String, String> key = new Pair<>("default", "test-run");
            SchedulerRestController.addScheduler( key, randomLAScheduler);
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

}
