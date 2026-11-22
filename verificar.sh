#!/usr/bin/env bash
# Operacion y verificacion de la EP5 en AWS (4 EC2). Se ejecuta en tu equipo (Linux, macOS o
# Git Bash en Windows) con AWS CLI configurado con las credenciales de AWS Academy.
# Los comandos remotos se ejecutan por SSM (aws ssm send-command): no se usa SSH ni puerto 22.
#
#   ./verificar.sh ayuda
set -uo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' PYTHONUTF8=1 AWS_PAGER=""

RAIZ="$(cd "$(dirname "$0")" && pwd)"
PROJECT="${PROJECT:-ep5-kafka}"
REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null | tr -d '\r')}"
REGION="${REGION:-us-east-1}"
tf() { (cd "$RAIZ/infra" && terraform "$@"); }
EVID="$RAIZ/docs/evidencias/txt"
ROLES="kafka-1 kafka-2 app monitoring"
TMPD="$(mktemp -d)"
trap 'rm -rf "$TMPD"' EXIT

# Comandos de Kafka dentro del contenedor del broker (sin el agente JMX: su puerto ya esta en uso)
PRE_KAFKA='K="docker exec -e KAFKA_OPTS= -e KAFKA_HEAP_OPTS=-Xmx256m kafka"; B=localhost:9092'
KCAT='docker run --rm --network host edenhill/kcat:1.7.1 -b localhost:9092'

c()    { printf '\n\033[1;36m==== %s ====\033[0m\n' "$*"; }
hora() { date -u +"[%H:%M:%S UTC]"; }
die()  { echo "ERROR: $*" >&2; exit 1; }
esperar() { local s=$1; shift; echo "... esperando ${s}s $*"; sleep "$s"; }

ruta() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else echo "$1"; fi; }

declare -A ID
id_de() {
  local rol=$1
  if [ -z "${ID[$rol]:-}" ]; then
    ID[$rol]=$(aws ec2 describe-instances --region "$REGION" \
      --filters "Name=tag:Project,Values=$PROJECT" "Name=tag:Role,Values=$rol" \
                "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].InstanceId' --output text | tr -d '\r' | awk '{print $1}')
    [ -n "${ID[$rol]}" ] || die "no hay EC2 con Role=$rol (¿terraform apply?)"
  fi
  echo "${ID[$rol]}"
}

# Bucket de despliegue: nombre fijo por cuenta (lo crea "desplegar" con AWS CLI, no Terraform)
bucket() {
  echo "$PROJECT-deploy-$(aws sts get-caller-identity --query Account --output text | tr -d '\r')"
}

# Sube deploy/ y scripts/ al bucket (sin .env ni .example)
subir_config() {
  local bk=$1
  aws s3 sync "$(ruta "$RAIZ/deploy")" "s3://$bk/deploy/" --region "$REGION" --delete \
      --exclude "*.env" --exclude "*.example" --exclude "app/build/*" --only-show-errors
  aws s3 sync "$(ruta "$RAIZ/scripts")" "s3://$bk/scripts/" --region "$REGION" --delete --only-show-errors
}

# remoto <rol> <script bash>: ejecuta el script en la EC2 por SSM y muestra su salida
remoto() {
  local rol=$1 script=$2 id f esc cid estado i
  id=$(id_de "$rol")
  f="$TMPD/cmd.json"
  esc=$(printf "bash <<'EP5EOF'\nset -o pipefail\n%s\nEP5EOF\n" "$script" \
        | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/\\t/g' | awk '{printf "%s\\n", $0}')
  printf '{"commands":["%s"],"executionTimeout":["%s"]}' "$esc" "${TIMEOUT_REMOTO:-900}" > "$f"
  cid=$(aws ssm send-command --region "$REGION" --instance-ids "$id" --document-name AWS-RunShellScript \
        --comment "ep5 $rol" --parameters "file://$(ruta "$f")" --query Command.CommandId --output text | tr -d '\r') \
    || die "SSM no acepto el comando en $rol"
  for i in $(seq 1 600); do
    sleep 2
    estado=$(aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$id" \
             --query Status --output text 2>/dev/null | tr -d '\r')
    case "$estado" in Success|Failed|Cancelled|TimedOut) break ;; esac
  done
  aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$id" \
      --query StandardOutputContent --output text | tr -d '\r'
  if [ "$estado" != "Success" ]; then
    echo "[estado SSM: $estado]"
    aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$id" \
        --query StandardErrorContent --output text | tr -d '\r' | tail -20
  fi
}

# ---------------------------------------------------------------------------------------------
cmd_desplegar() {
  command -v terraform >/dev/null || die "falta terraform"
  local ip bk
  ip=$(curl -fsS https://checkip.amazonaws.com | tr -d '\r\n ') || die "no pude detectar tu IP publica"
  bk=$(bucket)
  c "Bucket de despliegue s3://$bk (privado) con deploy/ y scripts/"
  if ! aws s3api head-bucket --bucket "$bk" --region "$REGION" 2>/dev/null; then
    aws s3api create-bucket --bucket "$bk" --region "$REGION" --output text >/dev/null || die "no se pudo crear el bucket"
  fi
  aws s3api put-public-access-block --bucket "$bk" --region "$REGION" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true 2>/dev/null || true
  subir_config "$bk" || die "no se pudo subir la configuracion"
  aws s3 ls "s3://$bk/" --recursive --region "$REGION" | wc -l | xargs echo "archivos en el bucket:"
  c "terraform apply (tu IP $ip/32 podra abrir Grafana, Kafka UI, Prometheus y la API)"
  tf init -input=false >/dev/null && tf apply -input=false -auto-approve -var "allowed_cidr=$ip/32" -var "bucket=$bk" \
    || die "terraform apply fallo"
  c "Siguiente paso"
  echo "El bootstrap de cada EC2 tarda ~5-8 min. Luego: ./verificar.sh subir-app && ./verificar.sh estado"
}

cmd_destruir() {
  local bk; bk=$(bucket)
  tf destroy -var "allowed_cidr=0.0.0.0/32" -var "bucket=$bk" && aws s3 rb "s3://$bk" --force --region "$REGION"
}

# Sube los cambios de deploy/ y scripts/ al bucket (despues: ./verificar.sh sincronizar <rol>)
cmd_subir_config() { local bk; bk=$(bucket); c "Subiendo deploy/ y scripts/ a s3://$bk"; subir_config "$bk" && echo OK; }

cmd_ec2() {
  c "EC2 del proyecto $PROJECT"
  aws ec2 describe-instances --region "$REGION" --filters "Name=tag:Project,Values=$PROJECT" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value, InstanceId, InstanceType, State.Name, Placement.AvailabilityZone, PrivateIpAddress, PublicIpAddress]' \
    --output table | tr -d '\r'
}

cmd_terraform() { c "Recursos creados por Terraform"; tf state list | tr -d '\r'; }

cmd_urls() {
  local ip
  ip=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$(id_de monitoring)" \
       --query 'Reservations[0].Instances[0].PublicIpAddress' --output text | tr -d '\r')
  c "Accesos (la IP publica cambia al apagar/encender; usuario admin)"
  echo "Grafana     http://$ip:3000   clave: terraform -chdir=infra output -raw grafana_password"
  echo "Kafka UI    http://$ip:8080   clave: terraform -chdir=infra output -raw kafka_ui_password"
  echo "Prometheus  http://$ip:9090"
  echo "API usuarios http://$(ip_publica app):8081/api/usuarios"
}

ip_publica() {
  aws ec2 describe-instances --region "$REGION" --instance-ids "$(id_de "$1")" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text | tr -d '\r'
}

# Prueba la API REST de usuarios (Flask) desde tu equipo: GET, POST, PUT, DELETE
cmd_api() {
  local base="http://$(ip_publica app):8081"
  c "API de usuarios en la EC2 app: $base"
  echo "--- GET /api/saludo";        curl -s "$base/api/saludo"; echo
  echo "--- GET /health";            curl -s "$base/health"; echo
  echo "--- GET /api/usuarios";      curl -s "$base/api/usuarios"; echo
  echo "--- POST /api/usuarios";     curl -s -X POST -H 'Content-Type: application/json' -d '{"nombre":"Usuario Prueba","email":"prueba@ejemplo.com"}' "$base/api/usuarios"; echo
  echo "--- PUT /api/usuarios/4";    curl -s -X PUT -H 'Content-Type: application/json' -d '{"nombre":"Usuario Editado"}' "$base/api/usuarios/4"; echo
  echo "--- GET /api/usuarios/4";    curl -s "$base/api/usuarios/4"; echo
  echo "--- DELETE /api/usuarios/4"; curl -s -X DELETE "$base/api/usuarios/4"; echo
  echo "--- GET /api/usuarios/99 (404)"; curl -s -w ' HTTP %{http_code}' "$base/api/usuarios/99"; echo
}

cmd_estado() {
  for rol in $ROLES; do
    c "$rol"
    remoto "$rol" 'cat /var/lib/ep5/bootstrap.status 2>/dev/null || echo "bootstrap aun no escribe estado"; docker ps --format "table {{.Names}}\t{{.Status}}"'
  done
}

cmd_kafka() {
  remoto kafka-1 "$PRE_KAFKA"'
echo "== Brokers registrados en ZooKeeper (deben ser [1, 2])"
docker exec -e KAFKA_OPTS= zookeeper zookeeper-shell localhost:2181 ls /brokers/ids 2>/dev/null | tail -1
echo; echo "== Brokers y su endpoint anunciado"
$K kafka-broker-api-versions --bootstrap-server $B 2>/dev/null | grep "(id:" | sed "s/ -> .*//" | sort
echo; echo "== Topics"
$K kafka-topics --bootstrap-server $B --list
echo; echo "== ventas-raw: lideres e ISR (Replicas 1,2 / Isr 1,2)"
$K kafka-topics --bootstrap-server $B --describe --topic ventas-raw
echo; echo "== Particiones sub-replicadas (vacio = OK)"
$K kafka-topics --bootstrap-server $B --describe --under-replicated-partitions'
}

cmd_topicos() {
  c "scripts/topicos.sh crear + describir + grupos (en kafka-1)"
  remoto kafka-1 'aws s3 sync "s3://$(cat /var/lib/ep5/bucket)/scripts/" /opt/ep5/scripts/ --only-show-errors
BOOTSTRAP=localhost:9092 KAFKA_CMD="docker exec -e KAFKA_OPTS= -e KAFKA_HEAP_OPTS=-Xmx256m kafka" bash /opt/ep5/scripts/topicos.sh todo'
}

cmd_inspeccionar() {
  c "Topics, internos de Kafka Streams, sub-replicadas y consumer groups (offset, fin, lag, asignacion)"
  remoto kafka-1 'BOOTSTRAP=localhost:9092 KAFKA_CMD="docker exec -e KAFKA_OPTS= -e KAFKA_HEAP_OPTS=-Xmx256m kafka" bash /opt/ep5/scripts/topicos.sh describir
BOOTSTRAP=localhost:9092 KAFKA_CMD="docker exec -e KAFKA_OPTS= -e KAFKA_HEAP_OPTS=-Xmx256m kafka" bash /opt/ep5/scripts/topicos.sh subreplicadas
BOOTSTRAP=localhost:9092 KAFKA_CMD="docker exec -e KAFKA_OPTS= -e KAFKA_HEAP_OPTS=-Xmx256m kafka" bash /opt/ep5/scripts/topicos.sh grupos'
}

cmd_sincronizar() {
  local rol=${1:-}
  [ -n "$rol" ] || die "uso: ./verificar.sh sincronizar <kafka-1|kafka-2|app|monitoring>"
  c "Re-sincroniza deploy/$rol desde S3 y aplica docker compose up -d"
  remoto "$rol" 'BK=$(cat /var/lib/ep5/bucket); R=$(ls /opt/ep5 | grep -E "^(kafka-1|kafka-2|app|monitoring)$")
aws s3 sync "s3://$BK/deploy/common/" /opt/ep5/common/ --only-show-errors
aws s3 sync "s3://$BK/deploy/$R/" /opt/ep5/$R/ --only-show-errors --exclude "build/*"
aws s3 sync "s3://$BK/scripts/" /opt/ep5/scripts/ --only-show-errors
cd /opt/ep5/$R
if [ -f prometheus.yml.tpl ]; then set -a; . ./.env; set +a; envsubst "\$KAFKA_1_IP \$KAFKA_2_IP \$APP_IP" < prometheus.yml.tpl > prometheus.yml; fi
if [ "$R" = app ] && [ ! -f build/target/ventas-app.jar ]; then docker compose up -d --build api-usuarios alloy; else docker compose up -d --build; fi
docker compose ps --format "table {{.Name}}\t{{.Status}}"'
}

cmd_subir_app() {
  local bk tgz
  bk=$(bucket); [ -n "$bk" ] || die "no encuentro el bucket (terraform output bucket)"
  c "Compilando la app (mvn package, incluye las pruebas)"
  (cd "$RAIZ/app" && env -u MSYS_NO_PATHCONV -u MSYS2_ARG_CONV_EXCL mvn -B -q package) || die "la compilacion o las pruebas fallaron"
  tgz="$TMPD/app.tgz"
  tar -czf "$tgz" -C "$RAIZ/app" Dockerfile jmx-app.yml target/ventas-app.jar
  c "Subiendo la app a s3://$bk/app/app.tgz ($(wc -c < "$tgz") bytes)"
  aws s3 cp "$(ruta "$tgz")" "s3://$bk/app/app.tgz" --region "$REGION" --only-show-errors || die "no se pudo subir a S3"
  c "Esperando el fin del bootstrap de la EC2 app y levantando producer + Kafka Streams"
  TIMEOUT_REMOTO=1500 remoto app 'for i in $(seq 1 90); do grep -q "BOOTSTRAP OK" /var/lib/ep5/bootstrap.status 2>/dev/null && break; sleep 10; done
cat /var/lib/ep5/bootstrap.status
BK=$(cat /var/lib/ep5/bucket)
aws s3 sync "s3://$BK/deploy/app/" /opt/ep5/app/ --only-show-errors --exclude "build/*"
aws s3 sync "s3://$BK/deploy/common/" /opt/ep5/common/ --only-show-errors
rm -rf /opt/ep5/app/build && mkdir -p /opt/ep5/app/build
aws s3 cp "s3://$BK/app/app.tgz" /tmp/app.tgz --only-show-errors && tar -xzf /tmp/app.tgz -C /opt/ep5/app/build
cd /opt/ep5/app
docker compose build --quiet 2>&1 | tail -3
docker compose up -d producer streams api-usuarios alloy 2>&1 | tail -8
sleep 25
echo; docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
echo; echo "--- producer"; docker compose logs --no-log-prefix --tail 4 producer
echo; echo "--- streams"; docker compose logs --no-log-prefix streams | grep -E "Configuracion|Kafka Streams:|RESTAURACION" | tail -12'
}

cmd_app() {
  remoto app 'cd /opt/ep5/app
echo "== Contenedores de la app"; docker compose ps --format "table {{.Name}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
echo; echo "== Ultimas ventas del producer (key = tiendaId)"; docker compose logs --no-log-prefix --tail 5 producer
echo; echo "== Configuracion y estado de Kafka Streams"; docker compose logs --no-log-prefix streams | grep -E "Configuracion|Kafka Streams:" | tail -6'
}

cmd_lectores() {
  remoto kafka-2 'for l in lector-ventas-raw lector-ventas-agregadas lector-ventas-dlq lector-ventas-descartadas; do echo "--- $l"; docker logs --tail 2 $l 2>&1 | cut -c1-260; done'
}

cmd_flujo() {
  c "ventas-raw: ultimas ventas con key, particion y offset"
  remoto kafka-2 "$KCAT"' -C -t ventas-raw -o -2 -e -q -f "particion=%p offset=%o key=%k\n  %s\n" | cut -c1-300'
  c "ventas-agregadas: ultimos agregados (inicio/fin de ventana, conteo, monto, ticket promedio)"
  remoto kafka-2 "$KCAT"' -C -t ventas-agregadas -o -3 -e -q -f "particion=%p offset=%o key=%k\n  %s\n"'
  c "Consumer group ventas-aggregator: offsets y lag"
  remoto kafka-1 "$PRE_KAFKA"'; $K kafka-consumer-groups --bootstrap-server $B --describe --group ventas-aggregator 2>/dev/null | awk "NF>0" | cut -c1-140'
}

cmd_casos() {
  local caso=${1:-todos}
  c "Casos controlados: $caso (tienda T-DEMO)"
  remoto app "cd /opt/ep5/app && docker compose run --rm -T casos casos $caso 2>&1 | grep -v SLF4J"
  if [ "$caso" = todos ]; then
    esperar 100 "a que cierren las ventanas de T-DEMO (60 s + 30 s de gracia)"
    cmd_resultados
  fi
}

cmd_resultados() {
  c "DLQ (ventas-raw-dlq): mensajes de T-DEMO con tipo, origen y valor original"
  remoto kafka-2 "$KCAT"' -C -t ventas-raw-dlq -o -15 -e -q -f "%p|%o|%k|%h|%s\n" | grep "|T-DEMO|" | tail -6 | python3 -c "
import sys, json
for l in sys.stdin:
    p, o, k, h, v = l.rstrip(\"\\n\").split(\"|\", 4)
    d = json.loads(v)
    print(\"%s origen=%s-%s@%s key=%s eventId=%s\" % (d[\"errorType\"], d[\"sourceTopic\"], d[\"sourcePartition\"], d[\"sourceOffset\"], d[\"originalKey\"], d.get(\"eventId\")))
    print(\"   motivo:   \" + d[\"error\"])
    print(\"   rawValue: \" + d[\"rawValue\"][:170])
"'
  c "Registro DLQ completo (el mas reciente de T-DEMO)"
  remoto kafka-2 "$KCAT"' -C -t ventas-raw-dlq -o -15 -e -q -f "%k|%s\n" | grep "^T-DEMO|" | tail -1 | cut -d"|" -f2- | python3 -m json.tool'
  c "ventas-descartadas: duplicados y tardios fuera de gracia de T-DEMO (no pasan por la DLQ)"
  remoto kafka-2 "$KCAT"' -C -t ventas-descartadas -o -10 -e -q -f "%s\n" | grep "\"tiendaId\":\"T-DEMO\"" | tail -4 | python3 -c "
import sys, json
for l in sys.stdin:
    d = json.loads(l)
    print(\"%s eventId=%s ventana=[%s, %s) streamTime=%s origen=%s-%s@%s\" % (d[\"motivo\"], d[\"eventId\"], d[\"windowStart\"], d[\"windowEnd\"], d[\"streamTime\"], d[\"sourceTopic\"], d[\"sourcePartition\"], d[\"sourceOffset\"]))
    print(\"   detalle: \" + d[\"detalle\"])
"'
  c "ventas-agregadas: ventanas de T-DEMO"
  remoto kafka-2 "$KCAT"' -C -t ventas-agregadas -o -40 -e -q -f "%s\n" | grep "\"tiendaId\":\"T-DEMO\"" | tail -4 | python3 -c "
import sys, json
for l in sys.stdin:
    d = json.loads(l)
    print(\"ventana [%s, %s) ventas=%s unidades=%s monto=%s ticketPromedio=%s\" % (d[\"windowStart\"], d[\"windowEnd\"], d[\"cantidadVentas\"], d[\"unidades\"], d[\"montoTotal\"], d[\"ticketPromedio\"]))
"'
  c "Log de Kafka Streams para T-DEMO (el contenedor sigue arriba)"
  remoto app 'cd /opt/ep5/app && docker compose logs --no-log-prefix --since 10m streams | grep -E "T-DEMO|demo-" | grep -vE "^$" | tail -14 | cut -c1-330; docker compose ps streams --format "{{.Name}} {{.Status}}"'
}

cmd_fallo() {
  local pre="$PRE_KAFKA"'
isr()   { $K kafka-topics --bootstrap-server $B --describe --topic ventas-raw; }
subrep(){ echo "Particiones sub-replicadas (todos los topics): $($K kafka-topics --bootstrap-server $B --describe --under-replicated-partitions | grep -c Partition)"; }
offs()  { echo "offset final ventas-raw (suma de particiones): $($K kafka-get-offsets --bootstrap-server $B --topic ventas-raw 2>/dev/null | awk -F: "{s+=\$3} END {print s}")"; }
lag()   { echo "lag de ventas-aggregator en ventas-raw: $($K kafka-consumer-groups --bootstrap-server $B --describe --group ventas-aggregator 2>/dev/null | awk "\$2==\"ventas-raw\" {s+=\$6} END {print s+0}")"; }'
  c "ANTES: ISR completo (Isr: 1,2) y lag"; hora
  remoto kafka-1 "$pre"$'\nisr; echo; subrep; offs; lag'
  c "Deteniendo el broker 2 (kafka-2) con produccion en curso"; hora
  remoto kafka-2 'docker stop kafka'
  esperar 20 "a que el controlador saque al broker 2 del ISR"
  c "DURANTE: ISR degradado (Isr: 1) y lideres movidos al broker 1"; hora
  remoto kafka-1 "$pre"$'\nisr; echo; subrep; offs'
  esperar 15 ""
  c "DURANTE (+15 s): el offset sigue creciendo = el productor escribe con acks=all y min.insync.replicas=1"; hora
  remoto kafka-1 "$pre"$'\noffs; lag'
  c "DURANTE: producer y Kafka Streams siguen arriba"
  remoto app 'cd /opt/ep5/app && docker compose ps producer streams --format "{{.Service}}  {{.Status}}"
docker compose logs --no-log-prefix --tail 2 producer | cut -c1-200
echo "errores en Kafka Streams durante la caida: $(docker compose logs --since 1m streams | grep -c " ERROR ")"
echo "errores de envio del productor durante la caida: $(docker compose logs --since 1m producer | grep -c "ERROR envio")"'
  c "Reiniciando el broker 2"; hora
  remoto kafka-2 'docker start kafka'
  c "DESPUES: el broker 2 se pone al dia y vuelve al ISR (Isr: 1,2)"
  remoto kafka-1 "$pre"$'\nfor i in $(seq 1 36); do n=$($K kafka-topics --bootstrap-server $B --describe --under-replicated-partitions | grep -c Partition); [ "$n" -eq 0 ] && break; sleep 5; done\ndate -u +"[%H:%M:%S UTC]"; isr; echo; subrep; offs; lag\necho "Nota: los lideres vuelven a repartirse con auto.leader.rebalance.enable (cada 300 s) o con kafka-leader-election --election-type preferred."'
}

cmd_reinicio_streams() {
  local id="reinicio-$(date -u +%H%M%S)"
  local pre="$PRE_KAFKA"$'\nlag() { $K kafka-consumer-groups --bootstrap-server $B --describe --group ventas-aggregator 2>/dev/null | awk \'NF>0\' | cut -c1-120; }'
  c "1) ANTES: offsets confirmados del grupo ventas-aggregator"; hora
  remoto kafka-1 "$pre"$'\nlag'
  c "2) Se envia una venta con eventId=$id"
  remoto app "cd /opt/ep5/app && docker compose run --rm -T casos casos reinicio $id 2>&1 | grep -v SLF4J"
  esperar 10 ""
  c "3) Se ELIMINA el contenedor de Kafka Streams (se pierde su RocksDB local) y se crea uno nuevo"; hora
  remoto app 'cd /opt/ep5/app && docker compose rm -sf streams 2>&1 | tail -3 && docker compose up -d streams 2>&1 | tail -2
for i in $(seq 1 60); do docker compose logs streams | grep -q -- "-> RUNNING" && break; sleep 3; done
docker compose logs --no-log-prefix streams | grep -E "Kafka Streams:|RESTAURACION fin" | grep -v "restaurados=0" | cut -c1-200
echo "registros restaurados en total: $(docker compose logs streams | grep "RESTAURACION fin" | sed "s/.*restaurados=//" | awk "{s+=\$1} END {print s+0}")"'
  c "4) Se REENVIA la misma venta: el store dedup-event-id restaurado la reconoce"
  remoto app "cd /opt/ep5/app && docker compose run --rm -T casos casos reinicio $id 2>&1 | grep -v SLF4J"
  esperar 10 ""
  remoto kafka-2 "$KCAT -C -t ventas-descartadas -o -20 -e -q -f '%s\n' | grep '$id' | python3 -c \"
import sys, json
for l in sys.stdin:
    d = json.loads(l); print(d['motivo'], d['eventId'], '-', d['detalle'])\""
  c "5) DESPUES: retoma desde los offsets confirmados (lag vuelve a 0)"; hora
  remoto kafka-1 "$pre"$'\nsleep 10; lag'
}

cmd_escalar() {
  c "Escalando Kafka Streams a 2 instancias (mismo application.id)"
  remoto app 'cd /opt/ep5/app && docker compose up -d --scale streams=2 streams 2>&1 | tail -3; sleep 45; docker compose ps streams --format "{{.Name}}  {{.Status}}  {{.Ports}}"'
  c "Reparto de particiones de ventas-raw entre las instancias"
  remoto kafka-1 "$PRE_KAFKA"'; $K kafka-consumer-groups --bootstrap-server $B --describe --group ventas-aggregator 2>/dev/null | awk "\$2==\"ventas-raw\" {split(\$7,a,\"-StreamThread\"); print \"particion \" \$3 \" -> instancia \" substr(a[1], length(a[1])-7)}" | sort'
  c "Vuelta a 1 instancia"
  remoto app 'cd /opt/ep5/app && docker compose up -d --scale streams=1 streams 2>&1 | tail -2; docker compose ps streams --format "{{.Name}}  {{.Status}}"'
}

cmd_conciliar() {
  c "Conciliacion por ventana: ventas validas unicas de ventas-raw vs suma de ventas-agregadas"
  remoto app "cd /opt/ep5/app && docker compose run --rm -T casos conciliar ${1:-20} 2>&1 | grep -v SLF4J"
}

cmd_monitoreo() {
  remoto monitoring 'echo "== Targets de Prometheus"
curl -s localhost:9090/api/v1/targets | python3 -c "
import sys, json
for t in json.load(sys.stdin)[\"data\"][\"activeTargets\"]:
    print(\"%-15s %-22s %s\" % (t[\"labels\"][\"job\"], t[\"labels\"].get(\"instance\", \"\"), t[\"health\"]))"
echo; echo "== Grafana"; curl -s localhost:3000/api/health; echo
echo; echo "== Loki: hosts que envian logs"; curl -s localhost:3100/loki/api/v1/label/host/values; echo
echo; echo "== Metricas clave (Prometheus)"
q() { curl -s -G localhost:9090/api/v1/query --data-urlencode "query=$1" | python3 -c "
import sys, json
r = json.load(sys.stdin)[\"data\"][\"result\"]
print(\"%-50s %s\" % (sys.argv[1], r[0][\"value\"][1] if r else \"sin datos\"))" "$2"; }
q "kafka_brokers" "brokers activos"
q "sum(kafka_topic_partition_under_replicated_partition)" "particiones sub-replicadas"
q "sum(kafka_consumergroup_lag{consumergroup=\"ventas-aggregator\",topic=\"ventas-raw\"})" "lag ventas-aggregator"
q "sum(rate(kafka_topic_partition_current_offset{topic=\"ventas-raw\"}[5m]))" "ventas/s en ventas-raw"
echo; echo "== Conteo de la ultima hora por motivo (logs de Kafka Streams en Loki)"
lq() { curl -s -G localhost:3100/loki/api/v1/query --data-urlencode "query=$1" | python3 -c "
import sys, json
for r in json.load(sys.stdin)[\"data\"][\"result\"]:
    print(\"   %-28s %s\" % (list(r[\"metric\"].values())[0] if r[\"metric\"] else \"total\", r[\"value\"][1]))"; }
lq "sum by (tipo) (count_over_time({service=\"streams\"} |= \"DLQ tipo=\" | regexp \"DLQ tipo=(?P<tipo>\\\\S+)\" [1h]))"
lq "sum by (motivo) (count_over_time({service=\"streams\"} |= \"DESCARTE motivo=\" | regexp \"DESCARTE motivo=(?P<motivo>\\\\S+)\" [1h]))"
echo "   TARDIO_EN_GRACIA (aceptados):"; lq "sum(count_over_time({service=\"streams\"} |= \"TARDIO_EN_GRACIA\" [1h]))"'
  cmd_urls
}

cmd_pruebas() {
  c "Pruebas automatizadas (mvn test)"
  (cd "$RAIZ/app" && env -u MSYS_NO_PATHCONV -u MSYS2_ARG_CONV_EXCL mvn -B test 2>&1) | grep -E "Tests run:|BUILD|FAIL"
  c "Detalle por prueba (target/surefire-reports)"
  for x in "$RAIZ"/app/target/surefire-reports/TEST-*.xml; do
    echo "$(basename "$x" .xml | sed 's/^TEST-//')"
    # testcase con <failure> o <error> = FALLA; si no, OK
    awk 'BEGIN{RS="<testcase "} NR>1 {
           match($0, /name="[^"]*"/); n=substr($0, RSTART+6, RLENGTH-7);
           gsub(/&quot;/, "\"", n); gsub(/&gt;/, ">", n); gsub(/&lt;/, "<", n); gsub(/&amp;/, "\\&", n);
           match($0, /time="[^"]*"/); t=substr($0, RSTART+6, RLENGTH-7);
           e = ($0 ~ /<(failure|error)/) ? "FALLA" : "OK  ";
           printf "  %s %s  (%s s)\n", e, n, t }' "$x"
  done
}

cmd_apagar() {
  local ids=""; for r in $ROLES; do ids="$ids $(id_de "$r")"; done
  c "Apagando las 4 EC2 (los datos quedan en los volumenes EBS)"
  aws ec2 stop-instances --region "$REGION" --instance-ids $ids --query 'StoppingInstances[].[InstanceId,CurrentState.Name]' --output text | tr -d '\r'
  aws ec2 wait instance-stopped --region "$REGION" --instance-ids $ids && echo "todas detenidas"
}

cmd_encender() {
  local ids=""; for r in $ROLES; do ids="$ids $(id_de "$r")"; done
  c "Encendiendo las 4 EC2 (las IP privadas no cambian; Docker levanta todo solo)"
  aws ec2 start-instances --region "$REGION" --instance-ids $ids --query 'StartingInstances[].[InstanceId,CurrentState.Name]' --output text | tr -d '\r'
  aws ec2 wait instance-status-ok --region "$REGION" --instance-ids $ids && echo "todas en running y status ok"
  esperar 60 "a que arranquen los contenedores"
  cmd_estado
  cmd_kafka
  c "Kafka Streams"
  remoto app 'cd /opt/ep5/app && docker compose logs --no-log-prefix --since 5m streams | grep "Kafka Streams:" | tail -3'
  cmd_urls
}

# Ejecuta un subcomando y guarda su salida (sin colores) en docs/evidencias/txt/<nombre>.txt
cmd_guardar() {
  local nombre=$1; shift
  mkdir -p "$EVID"
  { echo "\$ ./verificar.sh $*"; "$0" "$@"; } 2>&1 | tee >(sed 's/\x1b\[[0-9;]*m//g' > "$EVID/$nombre.txt")
}

cmd_evidencias() {
  cmd_guardar 01-ec2 ec2
  cmd_guardar 02-terraform terraform
  cmd_guardar 03-estado estado
  cmd_guardar 04-kafka kafka
  cmd_guardar 05-topicos topicos
  cmd_guardar 06-app app
  cmd_guardar 07-lectores lectores
  cmd_guardar 08-flujo flujo
  cmd_guardar 09-casos casos todos
  cmd_guardar 10-inspeccionar inspeccionar
  cmd_guardar 11-fallo-broker fallo
  cmd_guardar 12-conciliacion conciliar 20
  cmd_guardar 13-reinicio-streams reinicio-streams
  cmd_guardar 14-escalar escalar
  cmd_guardar 15-monitoreo monitoreo
  cmd_guardar 16-pruebas pruebas
}

ayuda() {
  cat <<'EOF'
uso: ./verificar.sh <comando>

Infraestructura
  desplegar            terraform apply (VPC, SG, bucket, 4 EC2); detecta tu IP publica
  ec2 | terraform      lista las EC2 / recursos de Terraform
  estado               bootstrap y contenedores de cada EC2
  urls                 Grafana, Kafka UI, Prometheus y API de usuarios
  api                  prueba la API REST de usuarios (Flask) en la EC2 app
  apagar | encender    detiene / inicia las 4 EC2 sin perder datos
  subir-config         sube los cambios de deploy/ y scripts/ al bucket
  sincronizar <rol>    re-aplica la configuracion de deploy/<rol> desde S3
  destruir             terraform destroy

Kafka y app
  subir-app            compila (con pruebas), sube y levanta producer + Kafka Streams
  kafka                brokers registrados, topics, ISR y sub-replicadas
  topicos              scripts/topicos.sh crear + describir + grupos
  inspeccionar         topics, internos de Streams, consumer groups, offsets, lag y asignacion
  app | lectores       estado de la app / ultimos mensajes de los lectores
  flujo                venta -> ventas-raw -> agregado en ventas-agregadas + lag

Escenarios
  casos [caso|todos]   casos controlados (casos lista en la app) y sus resultados
  resultados           DLQ, descartadas y agregados de T-DEMO
  fallo                detiene el broker 2 con produccion: ISR antes/durante/despues
  conciliar [n]        raw vs agregados por ventana (sin perdidas ni dobles conteos)
  reinicio-streams     elimina Kafka Streams y comprueba restauracion desde changelog
  escalar              2 instancias de Kafka Streams y reparto de particiones
  monitoreo            targets de Prometheus, Grafana, Loki y conteos por motivo
  pruebas              pruebas automatizadas (mvn test)
  evidencias           ejecuta todo y guarda la salida en docs/evidencias/txt/
EOF
}

sub="${1:-ayuda}"; shift || true
case "$sub" in
  desplegar|destruir|ec2|terraform|urls|api|estado|kafka|topicos|inspeccionar|app|lectores|flujo|casos|resultados|fallo|conciliar|escalar|monitoreo|pruebas|apagar|encender|evidencias|sincronizar|guardar)
    "cmd_$sub" "$@" ;;
  subir-app) cmd_subir_app "$@" ;;
  subir-config) cmd_subir_config "$@" ;;
  reinicio-streams) cmd_reinicio_streams "$@" ;;
  ayuda|-h|--help) ayuda ;;
  *) ayuda; exit 2 ;;
esac
