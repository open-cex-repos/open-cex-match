FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/*.jar app.jar
EXPOSE 9101
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
