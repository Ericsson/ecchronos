ARG JAVA_VERSION=17
FROM eclipse-temurin:${JAVA_VERSION}-jre

ARG PROJECT_VERSION=1.0.0-SNAPSHOT

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/ecchronos

ADD ecchronos-binary/target/ecchronos-binary-*.tar.gz .

WORKDIR /opt/ecchronos/ecchronos-binary-agent-${PROJECT_VERSION}

RUN ln -sf /var/log/ecchronos/ecchronos.log ecc.log

EXPOSE 8080

CMD ["bin/ecctool", "start", "-f"]

