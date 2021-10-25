package fonda.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.scheduler.RandomScheduler;
import lombok.extern.slf4j.Slf4j;
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
        log.info( "Started with namespace: {}", client.getNamespace() );
        new RandomScheduler("testscheduler", client, "default" );
    }

}
