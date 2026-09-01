ARG CASSANDRA_VERSION
FROM cassandra:${CASSANDRA_VERSION}

ARG JOLOKIA_VERSION=2.5.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl nginx openssl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L -o /opt/jolokia-jvm-agent.jar \
    "https://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-agent-jvm/${JOLOKIA_VERSION}/jolokia-agent-jvm-${JOLOKIA_VERSION}-javaagent.jar"

COPY nginx.conf /etc/nginx/nginx.conf.template
COPY ecc-entrypoint.sh /usr/local/bin/
RUN ln -s usr/local/bin/ecc-entrypoint.sh /ecc-entrypoint.sh

COPY create_keyspaces.cql /etc/cassandra/
COPY users.cql /etc/cassandra/
COPY setup_db.sh /etc/cassandra/

ENTRYPOINT ["ecc-entrypoint.sh"]
EXPOSE 7000 7001 7199 9042 8778
