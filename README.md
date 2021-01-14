# OpertusMundi BPM Worker Service

OpertusMundi worker service for executing external tasks for business process workflows.

## Quickstart

Copy configuration example files from `bpm-worker/config-example/` into `bpm-worker/src/main/resources/`, and edit to adjust to your needs.

`cp -r bpm-worker/config-example/* bpm-worker/src/main/resources/`

### Database configuration

Update database connection properties for each profile configuration file.

* application-development.properties
* application-production.properties

```properties
#
# Data source
#

spring.datasource.url = jdbc:postgresql://localhost:5432/camunda
spring.datasource.username = username
spring.datasource.password = password
spring.datasource.driver-class-name = org.postgresql.Driver
```

* application-testing.properties

```properties
#
# Data source
#

spring.datasource.url = jdbc:postgresql://localhost:5432/camunda-test
spring.datasource.username = username
spring.datasource.password = password
spring.datasource.driver-class-name = org.postgresql.Driver
```

### Camunda external task client configuration

By default the BPM server publishes the REST API endpoint at `http://localhost:8000/engine-rest`. The API is secured using Basic Authentication. The username and password values are the same with the ones used in the BPM server configuration.

```properties
opertus-mundi.bpm.worker.rest.base-url=http://localhost:8000/engine-rest
opertus-mundi.bpm.worker.rest.username=
opertus-mundi.bpm.worker.rest.password=
```

### Build

Build the project:

`mvn clean package`

### Run as standalone JAR

Run application (with an embedded Tomcat 9.x server) as a standalone application:

`java -jar bpm-worker/target/opertus-mundi-bpm-worker-1.0.0.jar`

or using the Spring Boot plugin:

`cd bpm-worker && mvn spring-boot:run`

The worker service requires an existing BPM server instance to successfully register for external tasks. 
