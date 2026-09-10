ARG CASSANDRA_VERSION
FROM cassandra:${CASSANDRA_VERSION}

# JOLOKIA_VERSION is intentionally required with no default. It must match the
# jolokia.adapter.version property in the root pom.xml so that the Jolokia agent
# attached to Cassandra and the jolokia-client-jmx-adapter used by ecChronos stay
# in lockstep. A mismatch is a real problem, so fail the build instead of silently
# falling back to a hardcoded (and potentially stale) default.
ARG JOLOKIA_VERSION

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl nginx openssl \
    && rm -rf /var/lib/apt/lists/*

RUN if [ -z "${JOLOKIA_VERSION}" ]; then \
        echo "ERROR: JOLOKIA_VERSION build arg is not set. It must be injected from the pom's jolokia.adapter.version." >&2; \
        exit 1; \
    fi \
    && echo "Using JOLOKIA_VERSION=${JOLOKIA_VERSION}" \
    && curl -fL -o /opt/jolokia-jvm-agent.jar \
    "https://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-agent-jvm/${JOLOKIA_VERSION}/jolokia-agent-jvm-${JOLOKIA_VERSION}-javaagent.jar"

COPY nginx.conf /etc/nginx/nginx.conf.template
COPY ecc-entrypoint.sh /usr/local/bin/
RUN ln -s usr/local/bin/ecc-entrypoint.sh /ecc-entrypoint.sh

COPY create_keyspaces.cql /etc/cassandra/
COPY users.cql /etc/cassandra/
COPY setup_db.sh /etc/cassandra/

ENTRYPOINT ["ecc-entrypoint.sh"]
EXPOSE 7000 7001 7199 9042 8778
