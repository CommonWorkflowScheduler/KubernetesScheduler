package fonda.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class Main {

    public static void main(String[] args) {
        if( System.getenv( "SCHEDULER_NAME" ) == null || System.getenv( "SCHEDULER_NAME" ).isEmpty() ){
            throw new IllegalArgumentException( "Please define environment variable: SCHEDULER_NAME" );
        }
        SpringApplication.run(Main.class, args);
    }

}
