package eu.opertusmundi.bpm.worker.config;

import org.camunda.bpm.client.ExternalTaskClient;
import org.camunda.bpm.client.interceptor.auth.BasicAuthProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ExternalTaskClientConfiguration {

    @Value("${opertus-mundi.bpm.worker.rest.base-url}")
    private String baseUrl;

    @Value("${opertus-mundi.bpm.worker.rest.username}")
    private String username;

    @Value("${opertus-mundi.bpm.worker.rest.password}")
    private String password;

    @Value("${opertus-mundi.bpm.worker.rest.timeout:20000}")
    private Long timeoutMillis;

    @Value("${opertus-mundi.bpm.worker.rest.max-tasks:10}")
    private Integer maxNumberOfTasks;

    @Bean(destroyMethod = "stop")
    public ExternalTaskClient externalTaskClient() {
        return ExternalTaskClient.create()
            .baseUrl(this.baseUrl)
            .maxTasks(this.maxNumberOfTasks)
            .asyncResponseTimeout(this.timeoutMillis)
            .addInterceptor(
                // See: https://docs.camunda.org/manual/latest/user-guide/ext-client/#request-interceptors
                new BasicAuthProvider(this.username, this.password)
            )
            .build();
    }

}
