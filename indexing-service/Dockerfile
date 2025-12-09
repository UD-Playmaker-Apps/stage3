FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

COPY target/indexing-service-1.0-SNAPSHOT.jar /app/indexing-service.jar

EXPOSE 7003

ENTRYPOINT ["java", "-jar", "/app/indexing-service.jar"]
