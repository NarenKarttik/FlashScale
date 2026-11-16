# ==============================================================================
# Multi-Stage Dockerfile for FlashScale
# Layered JAR extraction for blazing fast Docker caching & minimal attack surface
# ==============================================================================

# Stage 1: Build & Layer Extraction
FROM eclipse-temurin:21-jre-jammy AS extractor
WORKDIR /workspace
ARG JAR_FILE=target/flashscale-system-1.0.0.jar
COPY ${JAR_FILE} app.jar
RUN java -Djarmode=layertools -jar app.jar extract

# Stage 2: Production Distroless/Minimal Runtime
FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /app

# Run as non-root user for security compliance
RUN groupadd -r flashgroup && useradd -r -g flashgroup flashuser

# Copy extracted layers in order of frequency of change (least frequent first)
COPY --from=extractor /workspace/dependencies/ ./
COPY --from=extractor /workspace/spring-boot-loader/ ./
COPY --from=extractor /workspace/snapshot-dependencies/ ./
COPY --from=extractor /workspace/application/ ./

USER flashuser

# JVM Production Ergonomics for Containers:
# - MaxRAMPercentage=75% leaves 25% of memory for OS and off-heap/threads
# - Virtual threads optimize I/O concurrency
ENV JAVA_OPTS="-XX:+UseG1GC -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080 8081

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
