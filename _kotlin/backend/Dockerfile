FROM gradle:8.14.3-jdk21 AS build
WORKDIR /workspace
COPY settings.gradle.kts build.gradle.kts ./
COPY src ./src
RUN gradle --no-daemon bootJar

FROM eclipse-temurin:21-jre
RUN useradd --system --create-home --home-dir /app onyx
WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar /app/app.jar
RUN mkdir -p /var/lib/onyx/files && chown -R onyx:onyx /app /var/lib/onyx
USER onyx
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
