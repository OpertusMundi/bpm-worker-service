#!/bin/sh
set -u -e -o pipefail
set -x

function _gen_configuration()
{
    cat <<-EOD
	spring.datasource.url = jdbc:postgresql://${DATABASE_HOST}:${DATABASE_PORT:-5432}/${DATABASE_NAME}
	spring.datasource.username = ${DATABASE_USERNAME}
	spring.datasource.password = $(cat ${DATABASE_PASSWORD_FILE} | tr -d '\n')
	
	opertus-mundi.bpm.worker.rest.base-url = ${BPM_REST_BASE_URL}
	opertus-mundi.bpm.worker.rest.username = ${BPM_REST_USERNAME}
	opertus-mundi.bpm.worker.rest.password = $(cat ${BPM_REST_PASSWORD_FILE} | tr -d '\n')
	
	opertusmundi.feign.jwt.secret = $(cat ${JWT_SECRET_FILE} | tr -d '\n')
	
	opertusmundi.payments.mangopay.base-url = ${MANGOPAY_BASE_URL}
	opertusmundi.payments.mangopay.client-id = ${MANGOPAY_CLIENT_ID}
	opertusmundi.payments.mangopay.client-password = $(cat ${MANGOPAY_CLIENT_PASSWORD_FILE} | tr -d '\n')
	
	opertusmundi.feign.bpm-server.url = ${BPM_REST_BASE_URL} 
	opertusmundi.feign.bpm-server.basic-auth.username = ${BPM_REST_USERNAME}
	opertusmundi.feign.bpm-server.basic-auth.password = $(cat ${BPM_REST_PASSWORD_FILE} | tr -d '\n')
	
	opertusmundi.feign.catalogue.url = ${CATALOGUE_BASE_URL}
	
	opertusmundi.feign.ingest.url = ${INGEST_BASE_URL}
	
	opertusmundi.feign.transform.url = ${TRANSFORM_BASE_URL}
	
	opertusmundi.feign.email-service.url = ${MAILER_BASE_URL}
	opertusmundi.feign.email-service.jwt.subject = api-gateway
	
	opertusmundi.feign.message-service.url = ${MESSENGER_BASE_URL}
	opertusmundi.feign.message-service.jwt.subject = api-gateway
	
	opertusmundi.feign.rating-service.url = ${RATING_BASE_URL}
	opertusmundi.feign.rating-service.basic-auth.username = ${RATING_USERNAME}
	opertusmundi.feign.rating-service.basic-auth.password = $({ test -f ${RATING_PASSWORD_FILE} && cat ${RATING_PASSWORD_FILE}; } | tr -d '\n' || echo -n)
	
	opertusmundi.feign.data-profiler.url = ${DATA_PROFILER_BASE_URL}
	
	opertusmundi.feign.persistent-identifier-service.url= ${PID_BASE_URL}
	EOD
}

default_java_opts="-server -Djava.security.egd=file:///dev/urandom -Xms128m"
java_opts="${JAVA_OPTS:-${default_java_opts}}"

runtime_profile=$(hostname | md5sum | head -c 32)
_gen_configuration > ./config/application-${runtime_profile}.properties

exec java ${java_opts} -cp "/app/classes:/app/dependency/*" eu.opertusmundi.bpm.worker.Application \
  --spring.profiles.active=production,${runtime_profile}
