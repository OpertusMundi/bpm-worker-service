package eu.opertusmundi.bpm.worker;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;

@SpringBootApplication(
    scanBasePackageClasses = {
        eu.opertusmundi.common.config._Marker.class,
        eu.opertusmundi.common.repository._Marker.class,
        eu.opertusmundi.common.service._Marker.class,
        eu.opertusmundi.bpm.worker.config._Marker.class,
        eu.opertusmundi.bpm.worker.subscriptions._Marker.class,
    }
)
@EntityScan(
    basePackageClasses = {
        eu.opertusmundi.common.domain._Marker.class,
    }
)
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @PostConstruct
    private void initialized() {
        logger.info("Initialized");
    }

    @PreDestroy
    private void destroy() {
        logger.info("Shutting down");
    }

}
