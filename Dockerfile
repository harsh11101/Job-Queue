FROM eclipse-temurin:25-jdk-alpine AS build
ARG GRADLE_VERSION=9.5.1
WORKDIR /workspace
RUN apk add --no-cache curl unzip \
    && curl -fsSLo /tmp/gradle.zip "https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip" \
    && unzip -q /tmp/gradle.zip -d /opt \
    && ln -s "/opt/gradle-${GRADLE_VERSION}/bin/gradle" /usr/local/bin/gradle \
    && rm /tmp/gradle.zip
COPY build.gradle settings.gradle gradle.properties ./
COPY src ./src
RUN gradle --no-daemon clean bootJar

FROM eclipse-temurin:25-jre-alpine
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY --from=build /workspace/build/libs/*.jar /app/app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+UseZGC", "-jar", "/app/app.jar"]
