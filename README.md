# OpertusMundi BPM Worker Service

[![Build Status](https://ci.dev-1.opertusmundi.eu:9443/api/badges/OpertusMundi/bpm-worker-service/status.svg?ref=refs/heads/master)](https://ci.dev-1.opertusmundi.eu:9443/OpertusMundi/bpm-worker-service)

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

### Configure Feign clients

BPM worker service is using [Feign](https://cloud.spring.io/spring-cloud-openfeign/reference/html/) clients for connecting to other system services. For each service, an endpoint must be set and optionally security must be configured.

```properties
#
# Spring Cloud Feign clients
#

# Global secret for signing JWT tokens shared by all services
opertusmundi.feign.jwt.secret=

# Catalogue service (no authentication)
opertusmundi.feign.catalogue.url=

# BPM server (basic authentication)
opertusmundi.feign.bpm-server.url=
opertusmundi.feign.bpm-server.basic-auth.username=
opertusmundi.feign.bpm-server.basic-auth.password=

# Rating service (basic authentication)
opertusmundi.feign.rating-service.url=
opertusmundi.feign.rating-service.basic-auth.username=
opertusmundi.feign.rating-service.basic-auth.password=

# Email service (JWT token authentication)
# Uses private/public key pair for signing/parsing tokens.
opertusmundi.feign.email-service.url=

# Message service (JWT token authentication)
# Uses opertusmundi.feign.jwt.secret for signing tokens.
opertusmundi.feign.message-service.url=

# Ingest service
opertusmundi.feign.ingest.url=

# Transform service
opertusmundi.feign.transform.url=

# Data Profiler service
opertusmundi.feign.data-profiler.url=

# Persistent Identifier Service
opertusmundi.feign.persistent-identifier-service.url=

```

### Configure file system

BPM worker service requires access to the asset repository and user file system. The following directories must be accessible to the service:

```properties
#
# File system
#

# Folder for creating temporary files
opertusmundi.file-system.temp-dir=
# Root folder for storing user file system
opertusmundi.file-system.data-dir=
# Root folder for storing draft files
opertusmundi.file-system.draft-dir=
# Root folder for storing asset files
opertusmundi.file-system.asset-dir=
```

### Configure Payment service

BPM worker service supports external tasks that invoke MANGOPAY payment solution API.

```properties
#
# MangoPay
#

opertusmundi.payments.mangopay.base-url=
opertusmundi.payments.mangopay.client-id=
opertusmundi.payments.mangopay.client-password=
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
