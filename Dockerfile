# Multi-stage build for Synthea API (Java 11)

FROM eclipse-temurin:11-jdk AS builder
WORKDIR /workspace

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

# Sanity check: ensure Api.class exists in the fat jar
RUN test -f build/libs/*-api.jar \
 && jar tf build/libs/*-api.jar | grep -q 'org/mitre/synthea/simulator/Api.class' \
 || (echo "Api.class missing from fat jar" && exit 1)

FROM eclipse-temurin:11-jre AS runtime
WORKDIR /app

# Copy the fat JAR produced by the shadowApi task
# Be resilient to plugin naming: pick any JAR with the 'api' classifier
COPY --from=builder /workspace/build/libs/*-api.jar /app/synthea-api.jar

EXPOSE 8080

# Allow tuning the JVM if needed
ENV JAVA_OPTS="-Xms512m -Xmx2g"

# Run the API on port 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -cp /app/synthea-api.jar org.mitre.synthea.simulator.Api"]
