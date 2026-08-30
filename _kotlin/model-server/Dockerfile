# The model artifact is deliberately not downloaded or copied by this image.
# Mount an approved artifact and manifest at runtime; see README.md.
FROM gradle:8.14.4-jdk21 AS builder

WORKDIR /workspace
COPY build.gradle.kts settings.gradle.kts ./
COPY src ./src
RUN gradle --no-daemon bootJar

FROM eclipse-temurin:21-jre

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /workspace/build/libs/onyx-foss-kotlin-model-server-0.1.0.jar /app/app.jar

ENV MODEL_SERVER_PORT=9000
ENV MODEL_ARTIFACT_MANIFEST=
ENV HOME=/tmp

EXPOSE 9000

# /models is intentionally empty in the image. Operators mount an immutable,
# approved model package and point MODEL_ARTIFACT_MANIFEST at its manifest.
VOLUME ["/models"]

USER 10001
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
