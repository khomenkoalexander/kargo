# Accepts a pre-built app.jar (produced by: mvn -pl app -am package).
# Build context should be the docker/ directory with app.jar copied in.
# Example:
#   cp ../app/target/app.jar docker/app.jar
#   docker build -f docker/backend.Dockerfile -t kafkafile-backend docker/

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY app.jar /app/app.jar

EXPOSE 8080

ENV JAVA_OPTS="-Xmx1g -Xms250m"

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
