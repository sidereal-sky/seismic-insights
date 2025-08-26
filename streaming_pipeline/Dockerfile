FROM flink:2.1.0

RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN apt update -y && apt install -y \
    openjdk-17-jdk \
    openjdk-17-jre \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN JAVA_HOME_PATH=/usr/lib/jvm/java-17-openjdk-arm64 && \
    echo "export JAVA_HOME=$JAVA_HOME_PATH" >> /etc/environment && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/environment

ENV JAVA_HOME_PATH=/usr/lib/jvm/java-17-openjdk-arm64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /streaming_pipeline

COPY streaming_pipeline /streaming_pipeline
COPY timeseries_db /timeseries_db

RUN pip install --no-cache-dir -r /streaming_pipeline/requirements.txt

ENV JVM_ARGS="--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.nio.cs=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED --add-opens java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED"

RUN mkdir -p /opt && \
    curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar \
    -o /opt/flink-sql-connector-kafka.jar

CMD ["python", "/streaming_pipeline/main.py"]
