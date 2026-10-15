# Build from repository root after: mvn -Pdistribution -pl app -am package -DskipTests
# Supply a Linux Spark 4.1.1 / Scala 2.13 / Java 21 image with Delta 4.1.0 and
# the compatible Gluten Velox bundle built with -Pdelta (including VeloxDeltaComponent)
# installed as /opt/spark/jars/gluten.jar on ALL pods. Startup rejects generic bundles.
ARG SPARK_RUNTIME_IMAGE
FROM ${SPARK_RUNTIME_IMAGE}
USER root
WORKDIR /app
ENV JAVA_TOOL_OPTIONS="--enable-native-access=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
COPY app/target/distribution/cascada-app-0.1.0-SNAPSHOT.jar /app/cascada-app.jar
COPY app/target/distribution/lib/ /app/lib/
RUN java -XshowSettings:properties -version 2>&1 | grep -q 'java.specification.version = 21' && chown -R 185:0 /app
USER 185
EXPOSE 8000 9090
CMD ["java", "-cp", "/opt/spark/jars/gluten.jar:/app/cascada-app.jar:/app/lib/*:/opt/spark/jars/*:/opt/spark/work-dir:/opt/spark/conf", "com.cascada.app.bootstrap.CascadaLauncher"]
