# Imagen de la app (producer / streams / casos / conciliar). El jar se compila antes con:
#   mvn -B package        (o ./verificar.sh subir-app, que lo hace por ti)
FROM eclipse-temurin:17-jre
WORKDIR /app
# Exportador Prometheus de metricas JMX (Kafka clients y Kafka Streams)
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/jmx/jmx_prometheus_javaagent.jar
COPY jmx-app.yml /opt/jmx/jmx-app.yml
COPY target/ventas-app.jar /app/ventas-app.jar
ENV JAVA_OPTS="-Xms128m -Xmx384m"
# METRICS_PORT vacio = sin agente (p. ej. para "casos" y "conciliar")
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS ${METRICS_PORT:+-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=$METRICS_PORT:/opt/jmx/jmx-app.yml} -jar /app/ventas-app.jar \"$@\"", "--"]
CMD ["ayuda"]
