# syntax=docker/dockerfile:1.7
# One Dockerfile for all services:  docker build --build-arg SERVICE=exception-service -t demp/exception-service .
# The jar is built by the CI "build" job (mvn package) and only unpacked here, so the image build needs no Maven.

FROM eclipse-temurin:21-jre-alpine AS layers
ARG SERVICE
WORKDIR /build
COPY services/${SERVICE}/target/${SERVICE}.jar app.jar
# Spring Boot layered jar: dependencies change rarely -> separate, well cached image layers
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination extracted

FROM eclipse-temurin:21-jre-alpine
ARG SERVICE
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="${SERVICE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
RUN addgroup -S app -g 10001 && adduser -S app -u 10001 -G app
WORKDIR /app
COPY --from=layers /build/extracted/dependencies/ ./
COPY --from=layers /build/extracted/spring-boot-loader/ ./
COPY --from=layers /build/extracted/snapshot-dependencies/ ./
COPY --from=layers /build/extracted/application/ ./
USER 10001:10001
EXPOSE 8080
ENV SERVER_PORT=8080 \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError" \
    LOGGING_STRUCTURED_FORMAT_CONSOLE=ecs
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
