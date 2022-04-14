package eu.opertusmundi.bpm.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;

@SpringBootApplication(
    scanBasePackageClasses = {
        eu.opertusmundi.common.config._Marker.class,
        eu.opertusmundi.common.repository._Marker.class,
        eu.opertusmundi.common.service._Marker.class,
        eu.opertusmundi.common.support._Marker.class,
        eu.opertusmundi.common.util._Marker.class,
        eu.opertusmundi.bpm.worker.config._Marker.class,
        eu.opertusmundi.bpm.worker.subscriptions._Marker.class,
        eu.opertusmundi.bpm.worker.support._Marker.class,
    }
)
@EntityScan(
    basePackageClasses = {
        eu.opertusmundi.common.domain._Marker.class,
    }
)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
