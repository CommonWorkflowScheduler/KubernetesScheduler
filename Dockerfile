FROM eclipse-temurin:21-jdk-jammy AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*

COPY pom.xml .
RUN mvn dependency:go-offline --no-transfer-progress

COPY src/ ./src/
RUN mvn package --no-transfer-progress -DskipTests

FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/cws-k8s-scheduler-*-SNAPSHOT.jar app.jar

CMD ["java", "-jar", "app.jar"]
