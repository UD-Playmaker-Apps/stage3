
# indexing-service/Dockerfile
FROM eclipse-temurin:17-jdk-alpine
WORKDIR /app
COPY target/indexing-service-1.0.0-shaded.jar app.jar
ENV JAVA_OPTS=""
EXPOSE 7002
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar app.jar"]
