FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

COPY target/search-service-1.0-SNAPSHOT.jar /app/search-service.jar

EXPOSE 7004

ENTRYPOINT ["java", "-jar", "/app/search-service.jar"]
