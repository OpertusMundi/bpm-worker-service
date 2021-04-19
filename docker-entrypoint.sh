#!/bin/sh
set -u -e -o pipefail

[[ "${DEBUG:-f}" != "f" || "${XTRACE:-f}" != "f" ]] && set -x


function _valid_http_url()
{
    grep -q -e "^\(http\|https\):[/][/]" || { echo "$1 does not seem like an http(s) URL" 1>&2 && false; }
}

function _valid_jdbc_url()
{
    grep -q -e "^jdbc:postgresql:[/][/]" || { echo "$1 does not seem like a JDBC PostgreSQL connection string" 1>&2 && false; }
}

function _gen_configuration()
{    
    echo "${DATABASE_URL}" | _valid_jdbc_url "DATABASE_URL"
    database_url=${DATABASE_URL}
    database_username=${DATABASE_USERNAME}
    database_password=$(cat ${DATABASE_PASSWORD_FILE} | tr -d '\n')
    
    jwt_secret=$(cat ${JWT_SECRET_FILE} | tr -d '\n')
    
    echo "${BPM_REST_BASE_URL}" | _valid_http_url "BPM_REST_BASE_URL" 
    bpm_rest_base_url=${BPM_REST_BASE_URL}
    bpm_rest_username=${BPM_REST_USERNAME}
    bpm_rest_password=$(cat ${BPM_REST_PASSWORD_FILE} | tr -d '\n')

    echo ${MANGOPAY_BASE_URL} | _valid_http_url "MANGOPAY_BASE_URL"
    mangopay_base_url=${MANGOPAY_BASE_URL}
    mangopay_client_id=${MANGOPAY_CLIENT_ID}
    mangopay_client_password=$(cat ${MANGOPAY_CLIENT_PASSWORD_FILE} | tr -d '\n')

    echo ${CATALOGUE_BASE_URL} | _valid_http_url "CATALOGUE_BASE_URL"
    catalogue_base_url=${CATALOGUE_BASE_URL}
   
    test -z "${INGEST_BASE_URL}" || \
        echo ${INGEST_BASE_URL} | _valid_http_url "INGEST_BASE_URL"
    ingest_base_url=${INGEST_BASE_URL}
   
    test -z "${TRANSFORM_BASE_URL}" || \
        echo ${TRANSFORM_BASE_URL} | _valid_http_url "TRANSFORM_BASE_URL"
    transform_base_url=${TRANSFORM_BASE_URL}
    
    test -z "${MAILER_BASE_URL}" || \
        echo ${MAILER_BASE_URL} | _valid_http_url "MAILER_BASE_URL"
    mailer_base_url=${MAILER_BASE_URL}
    
    test -z "${MESSENGER_BASE_URL}" || \
        echo ${MESSENGER_BASE_URL} | _valid_http_url "MESSENGER_BASE_URL"
    messenger_base_url=${MESSENGER_BASE_URL}
  
    test -z "${RATING_BASE_URL}" || \
        echo ${RATING_BASE_URL} | _valid_http_url "RATING_BASE_URL"
    rating_base_url=${RATING_BASE_URL}
    rating_username=${RATING_USERNAME}
    rating_password=$({ test -f "${RATING_PASSWORD_FILE}" && \
        cat ${RATING_PASSWORD_FILE}; } | tr -d '\n' || echo -n)

    test -z "${PROFILE_BASE_URL}" || \
        echo ${PROFILE_BASE_URL} | _valid_http_url "PROFILE_BASE_URL"
    profile_base_url=${PROFILE_BASE_URL}

    test -z "${PID_BASE_URL}" || \
        echo ${PID_BASE_URL} | _valid_http_url "PID_BASE_URL"
    pid_base_url=${PID_BASE_URL}

    cat <<-EOD
	spring.datasource.url = ${database_url}
	spring.datasource.username = ${database_username}
	spring.datasource.password = ${database_password}
	
	opertus-mundi.bpm.worker.rest.base-url = ${bpm_rest_base_url}
	opertus-mundi.bpm.worker.rest.username = ${bpm_rest_username}
	opertus-mundi.bpm.worker.rest.password = ${bpm_rest_password}
	
	opertusmundi.feign.jwt.secret = ${jwt_secret}
	
	opertusmundi.payments.mangopay.base-url = ${mangopay_base_url}
	opertusmundi.payments.mangopay.client-id = ${mangopay_client_id}
	opertusmundi.payments.mangopay.client-password = ${mangopay_client_password}
	
	opertusmundi.feign.bpm-server.url = ${bpm_rest_base_url} 
	opertusmundi.feign.bpm-server.basic-auth.username = ${bpm_rest_username}
	opertusmundi.feign.bpm-server.basic-auth.password = ${bpm_rest_password}
	
	opertusmundi.feign.catalogue.url = ${catalogue_base_url}
	
	opertusmundi.feign.ingest.url = ${ingest_base_url}
	
	opertusmundi.feign.transform.url = ${transform_base_url}
	
	opertusmundi.feign.email-service.url = ${mailer_base_url}
	opertusmundi.feign.email-service.jwt.subject = api-gateway
	
	opertusmundi.feign.message-service.url = ${messenger_base_url}
	opertusmundi.feign.message-service.jwt.subject = api-gateway
	
	opertusmundi.feign.rating-service.url = ${rating_base_url}
	opertusmundi.feign.rating-service.basic-auth.username = ${rating_username}
	opertusmundi.feign.rating-service.basic-auth.password = ${rating_password}
	
	opertusmundi.feign.data-profiler.url = ${profile_base_url}
	
	opertusmundi.feign.persistent-identifier-service.url= ${pid_base_url}
	EOD
}

runtime_profile=$(hostname | md5sum | head -c 32)
_gen_configuration > ./config/application-${runtime_profile}.properties

# Run

main_class=eu.opertusmundi.bpm.worker.Application
default_java_opts="-server -Djava.security.egd=file:///dev/urandom -Xms128m"
exec java ${JAVA_OPTS:-${default_java_opts}} -cp "/app/classes:/app/dependency/*" ${main_class} \
  --spring.profiles.active=production,${runtime_profile}
