# vim: set syntax=dockerfile:

#FROM maven:3.6.3-openjdk-8 as build-stage-1
# see https://github.com/OpertusMundi/java-commons/blob/master/Dockerfile
FROM opertusmundi/java-commons-builder:1.0.55 as build-stage-1

WORKDIR /app

COPY common /app/common/
RUN (cd /app/common && mvn -B install)

COPY pom.xml /app/
COPY bpm-worker/pom.xml /app/bpm-worker/
RUN mvn -B dependency:resolve-plugins dependency:resolve
RUN mvn -B -pl bpm-worker dependency:copy-dependencies -DincludeScope=runtime

COPY bpm-worker/src /app/bpm-worker/src/
COPY bpm-worker/resources /app/bpm-worker/resources
RUN mvn -B compile -DenableDockerBuildProfile


FROM openjdk:8-jre-alpine

COPY --from=build-stage-1 /app/bpm-worker/target/ /app/

RUN addgroup spring && adduser -H -D -G spring spring

COPY docker-entrypoint.sh /usr/local/bin
RUN chmod a+x /usr/local/bin/docker-entrypoint.sh

WORKDIR /app

RUN mkdir config logs \
    && chgrp spring config logs \
    && chmod g=rwx config logs

ENV DATABASE_URL="jdbc:postgresql://db:5432/opertusmundi" \
    DATABASE_USERNAME="spring" \
    DATABASE_PASSWORD_FILE="/secrets/database-password" \
    JWT_SECRET_FILE="/secrets/jwt-signing-key" \
    BPM_REST_BASE_URL="http://bpm-server:8000/engine-rest" \
    BPM_REST_USERNAME="" \
    BPM_REST_PASSWORD_FILE="/secrets/bpm-rest-password" \
    MANGOPAY_BASE_URL="https://api.mangopay.com" \
    MANGOPAY_CLIENT_ID="" \
    MANGOPAY_CLIENT_PASSWORD_FILE="/secrets/mangopay-client-password" \
    CATALOGUE_BASE_URL="http://catalogueapi:8000/" \
    INGEST_BASE_URL="http://ingest:8000/" \
    TRANSFORM_BASE_URL="http://transform:8000/" \
    MAILER_BASE_URL="http://mailer:8000/" \
    MESSENGER_BASE_URL="http://messenger:8000/" \
    RATING_BASE_URL="http://rating:8000/" \
    RATING_USERNAME="" \
    RATING_PASSWORD_FILE="/secrets/rating-password" \
    PROFILE_BASE_URL="http://profile:8000/" \
    PID_BASE_URL="http://pid:8000/" \
    ELASTICSEARCH_BASE_URL="http://elasticsearch:9200" \
    ELASTICSEARCH_INDICES_ASSETS_INDEX_NAME="assets" \
    ELASTICSEARCH_INDICES_PROFILES_INDEX_NAME="profiles"

VOLUME [ \
    "/var/local/opertusmundi/files/assets", \
    "/var/local/opertusmundi/files/users", \
    "/var/local/opertusmundi/files/drafts", \
    "/var/local/opertusmundi/files/temp", \
    "/var/local/opertusmundi/ingest/input" ]

USER spring
ENTRYPOINT [ "/usr/local/bin/docker-entrypoint.sh" ]
