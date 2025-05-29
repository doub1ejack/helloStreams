FROM openjdk:8-jdk-slim

# Install sbt
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add - && \
    apt-get update && apt-get install -y sbt

WORKDIR /app

# Copy build definition
COPY build.sbt .
COPY src ./src

# Compile to cache dependencies
RUN sbt compile

# Run by default
CMD ["sbt", "run"]