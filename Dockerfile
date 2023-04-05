FROM maven:3-openjdk-18-slim AS build
WORKDIR /build
COPY pom.xml pom.xml
RUN mvn dependency:go-offline --no-transfer-progress -Dmaven.repo.local=/mvn/.m2nrepo/repository
COPY src/ src/
#RUN mvn package -DskipTests -Dmaven.repo.local=/mvn/.m2nrepo/repository

RUN mvn package --no-transfer-progress -f /build/pom.xml -DskipTests -Dmaven.repo.local=/mvn/.m2nrepo/repository

ENTRYPOINT ["/bin/sh","-c", "mvn spring-boot:run -f pom.xml -Dmaven.repo.local=/mvn/.m2nrepo/repository"]

#
# Package stage
#
#FROM openjdk:18-alpine
#WORKDIR /app
#RUN apk add --no-cache libstdc++
#RUN addgroup -S javagroup && adduser -S javauser -G javagroup && mkdir data
#COPY --from=build /build/target/k8s-scheduler*.jar k8s-scheduler.jar
#RUN chown -R javauser:javagroup /app
#USER javauser
#EXPOSE 8080
#ENTRYPOINT ["java","-jar","/app/k8s-scheduler.jar"]