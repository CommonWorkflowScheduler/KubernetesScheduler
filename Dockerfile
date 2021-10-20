FROM maven:3.8.3-jdk-11-slim AS build
WORKDIR /build
COPY pom.xml pom.xml
RUN mvn dependency:go-offline -B -Dmaven.repo.local=/mvn/.m2nrepo/repository
COPY src/ src/
RUN mvn package -Dmaven.repo.local=/mvn/.m2nrepo/repository

#
# Package stage
#
FROM openjdk:11-jre-slim
WORKDIR /app
RUN groupadd -r javauser && useradd --no-log-init -r -g javauser javauser
COPY --from=build /build/target/k8s-scheduler*.jar k8s-scheduler.jar
RUN chown -R javauser:javauser /app
USER javauser
EXPOSE 8080
ENTRYPOINT ["java","-jar","k8s-scheduler.jar"]