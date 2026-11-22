# Plantilla: el bootstrap reemplaza las IP con envsubst y genera prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: kafka
    static_configs:
      - targets: ["${KAFKA_1_IP}:7071"]
        labels: {instance: kafka-1}
      - targets: ["${KAFKA_2_IP}:7071"]
        labels: {instance: kafka-2}

  - job_name: zookeeper
    static_configs:
      - targets: ["${KAFKA_1_IP}:7072"]
        labels: {instance: zookeeper}

  # ISR, particiones sub-replicadas, offsets y lag por consumer group
  - job_name: kafka-exporter
    static_configs:
      - targets: ["kafka-exporter:9308"]

  - job_name: producer
    static_configs:
      - targets: ["${APP_IP}:9406"]
        labels: {instance: producer}

  # 9407 = instancia 1 de Kafka Streams, 9408 = instancia 2 (escalamiento)
  - job_name: streams
    static_configs:
      - targets: ["${APP_IP}:9407", "${APP_IP}:9408"]
