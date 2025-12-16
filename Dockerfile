# Multi-stage build for Synthea API (Java 11)

FROM eclipse-temurin:11-jdk AS builder
WORKDIR /workspace

# Git is needed by synthea versionTxt task
RUN apt-get update && \
    apt-get install -y git

# Leverage Gradle wrapper; copy minimal files first for better caching
COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY build.gradle settings.gradle ./
COPY gradle ./gradle

RUN chmod +x gradlew

# Copy sources and required resources
COPY src ./src
COPY lib ./lib
COPY config ./config

# Build the runnable fat JAR for the API
RUN ./gradlew shadowApi

# Unzip the JAR
RUN (cd build/libs && jar -xf synthea-api.jar && rm synthea-api.jar)

FROM eclipse-temurin:11-jre AS runtime
WORKDIR /app

# Copy the fat JAR produced by the shadowApi task
# Be resilient to plugin naming: pick any JAR with the 'api' classifier
COPY --from=builder /workspace/build/libs /app/synthea-api

EXPOSE 8080

# Allow tuning the JVM if needed
ENV JAVA_OPTS="-Xms512m -Xmx2g"

# Run the API on port 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -cp /app/synthea-api/ org.mitre.synthea.simulator.Api"]
