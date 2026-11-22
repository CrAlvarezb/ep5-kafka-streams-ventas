#!/usr/bin/env bash
# Crea e inspecciona los topics y consumer groups del encargo. Idempotente: se puede repetir.
#
# Uso:  topicos.sh crear | describir | grupos | subreplicadas | todo
# Variables:
#   BOOTSTRAP   servidores Kafka (por defecto localhost:9092)
#   KAFKA_CMD   prefijo para ejecutar las herramientas de Kafka. En las EC2 se usan las del
#               contenedor del broker:  KAFKA_CMD="docker exec -i -e KAFKA_OPTS= kafka"
#               En el plan B local (contenedor con las herramientas) va vacio.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
K="${KAFKA_CMD:-}"
APP_ID="${APPLICATION_ID:-ventas-aggregator}"

DIA=86400000
# topic | particiones | retencion (ms) | justificacion
TOPICS=(
  "ventas-raw|4|$((7 * DIA))|entrada; 4 particiones = paralelismo maximo de 4 instancias de Streams; 7 dias para reprocesar"
  "ventas-agregadas|2|$((30 * DIA))|salida pequena (1 registro por tienda y minuto); 30 dias de historia"
  "ventas-raw-dlq|4|$((14 * DIA))|mismas particiones que la entrada; 14 dias para corregir y reinyectar"
  "ventas-descartadas|2|$((14 * DIA))|auditoria de duplicados y tardios fuera de gracia"
)
# Configuracion comun: RF 2 (hay 2 brokers: RF 3 es imposible), min ISR 1 (con un broker caido se
# sigue escribiendo con acks=all), sin eleccion "unclean" (una replica atrasada no puede ser lider).
RF=2
CONFIG_COMUN="min.insync.replicas=1,unclean.leader.election.enable=false,cleanup.policy=delete"

kt()  { $K kafka-topics --bootstrap-server "$BOOTSTRAP" "$@"; }
kc()  { $K kafka-configs --bootstrap-server "$BOOTSTRAP" "$@"; }
kcg() { $K kafka-consumer-groups --bootstrap-server "$BOOTSTRAP" "$@"; }
titulo() { printf '\n== %s\n' "$*"; }

crear() {
  for def in "${TOPICS[@]}"; do
    IFS='|' read -r nombre particiones retencion porque <<<"$def"
    kt --create --if-not-exists --topic "$nombre" --partitions "$particiones" --replication-factor "$RF" \
       --config "retention.ms=$retencion" >/dev/null
    # Aplica (o re-aplica) la configuracion explicita aunque el topic ya existiera
    kc --alter --entity-type topics --entity-name "$nombre" \
       --add-config "retention.ms=$retencion,$CONFIG_COMUN"
    printf '   %-20s particiones=%s RF=%s retencion=%sd  (%s)\n' "$nombre" "$particiones" "$RF" "$((retencion / DIA))" "$porque"
  done
}

describir() {
  titulo "Brokers del cluster (deben aparecer id: 1 e id: 2)"
  $K kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" 2>/dev/null | grep -E '^[^ ].*\(id:' | sed 's/ -> .*//' | sort
  for def in "${TOPICS[@]}"; do
    IFS='|' read -r nombre _ <<<"$def"
    titulo "$nombre: particiones, lider, replicas e ISR"
    kt --describe --topic "$nombre"
    echo "-- configuracion explicita del topic"
    # Una linea por clave configurada en el topic (sin la lista de sinonimos)
    kc --describe --entity-type topics --entity-name "$nombre" | awk '/^  [a-z]/ {print "   " $1}'
  done
  titulo "Topics internos de Kafka Streams (repartition / changelog)"
  kt --describe 2>/dev/null | grep -E "^Topic: ${APP_ID}-" | awk '{printf "   %-62s particiones=%s RF=%s\n", $2, $6, $8}' || echo "   (aun no existen: se crean al iniciar Kafka Streams)"
}

subreplicadas() {
  titulo "Particiones sub-replicadas (vacio = todas con ISR completo)"
  kt --describe --under-replicated-partitions
}

grupos() {
  titulo "Consumer groups"
  kcg --list | sort
  for g in $(kcg --list | sort); do
    titulo "Grupo $g: offset confirmado, offset final, lag y consumidor asignado por particion"
    kcg --describe --group "$g" 2>/dev/null | awk 'NF>0 && $1!="GROUP" {printf "%-48s %-4s %10s %10s %6s  %s\n", $2, $3, $4, $5, $6, $7} $1=="GROUP" {printf "%-48s %-4s %10s %10s %6s  %s\n", "TOPIC", "PART", "OFFSET", "FIN", "LAG", "CONSUMER-ID"}'
  done
}

case "${1:-todo}" in
  crear)         crear ;;
  describir)     describir ;;
  subreplicadas) subreplicadas ;;
  grupos)        grupos ;;
  todo)          crear; describir; subreplicadas; grupos ;;
  *) echo "uso: $0 crear|describir|subreplicadas|grupos|todo" >&2; exit 2 ;;
esac
