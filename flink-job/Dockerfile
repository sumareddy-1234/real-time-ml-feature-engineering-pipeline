# Stage 1: Build the Java application using Maven (Java 17 builder compiling to --release 11)
FROM maven:3.9.6-eclipse-temurin-17-alpine AS builder

WORKDIR /app

# Copy pom.xml first to leverage Docker layer caching for dependencies
COPY pom.xml .

# Copy source and build the fat JAR (downloads deps + compiles in one step)
COPY src ./src
RUN mvn package -DskipTests -B

# Stage 2: Flink job submitter image (ships with Java 11)
FROM flink:1.20

WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /app/target/flink-job-1.0-SNAPSHOT.jar /app/flink-job.jar

# Copy start script
COPY start.sh /app/start.sh

# Change user to root to modify permissions
USER root
RUN chmod +x /app/start.sh

ENTRYPOINT ["/app/start.sh"]