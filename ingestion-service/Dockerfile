FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

COPY target/ingestion-service-1.0-SNAPSHOT.jar /app/ingestion-service.jar

EXPOSE 7001

ENTRYPOINT ["java", "-jar", "/app/ingestion-service.jar"]
