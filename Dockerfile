FROM maven:3-openjdk-17-slim AS build
WORKDIR /build
COPY pom.xml pom.xml
RUN mvn dependency:go-offline --no-transfer-progress -Dmaven.repo.local=/mvn/.m2nrepo/repository
COPY src/ src/
#RUN mvn package -DskipTests -Dmaven.repo.local=/mvn/.m2nrepo/repository

RUN mvn package --no-transfer-progress -f /build/pom.xml -DskipTests -Dmaven.repo.local=/mvn/.m2nrepo/repository

ENTRYPOINT ["/bin/sh","-c", "mvn spring-boot:run -f pom.xml -Dmaven.repo.local=/mvn/.m2nrepo/repository"]