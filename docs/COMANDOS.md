# Comandos de la EP5 explicados

DSY1107 · Desarrollo Cloud Native I · Evaluación Parcial N° 5

Este documento reúne todos los comandos usados para levantar, operar y probar el sistema, con lo que hace cada uno, dónde se ejecuta y qué se debe ver en la salida.

## 1. Dónde se ejecuta cada cosa

| Lugar | Cómo se entra | Para qué |
|---|---|---|
| Tu PC (Git Bash o terminal de VS Code) | abrir Git Bash en la carpeta del proyecto | credenciales, Terraform, compilar, `./verificar.sh` |
| EC2 kafka-1 (ZooKeeper + broker 1) | Consola AWS → EC2 → ep5-kafka-kafka-1 → Conectar → Session Manager, luego `sudo -i` | comandos de Kafka: topics, grupos, offsets, consumir y producir mensajes |
| EC2 kafka-2 (broker 2) | igual, con ep5-kafka-kafka-2 | detener y levantar el broker 2 en la prueba de falla |
| EC2 app (producer + Kafka Streams + API) | igual, con ep5-kafka-app | logs de la app, casos controlados, reinicio de Streams |
| EC2 monitoring | igual, con ep5-kafka-monitoring | Prometheus, Grafana, Loki, Kafka UI |

Regla práctica: `./verificar.sh` se ejecuta en tu PC y, por dentro, envía los comandos de Kafka a la EC2 que corresponde mediante AWS SSM (sin SSH ni puerto 22). Las secciones 6 a 10 muestran esos mismos comandos para ejecutarlos a mano dentro de cada EC2.

## 2. Credenciales de AWS Academy (en tu PC)

Las credenciales del Learner Lab cambian en cada sesión. Se copian desde AWS Details → AWS CLI: Show y se cargan en tu PC, nunca en el repositorio.

```bash
aws configure
```
Pide Access Key ID, Secret Access Key, región (`us-east-1`) y formato (`json`).

```bash
aws configure set aws_session_token PEGA_AQUI_EL_TOKEN
```
`aws configure` no pide el token de sesión, y AWS Academy lo exige. Este comando lo agrega.

```bash
aws sts get-caller-identity
```
Comprueba que las credenciales sirven. Debe responder con un `Arn` del tipo `assumed-role/voclabs/...`. Si responde `InvalidClientTokenId`, las claves son de una sesión anterior del lab.

## 3. Infraestructura con Terraform (en tu PC)

```bash
./verificar.sh desplegar
```
Hace tres cosas:
1. Detecta tu IP pública, que es la única que podrá abrir Grafana, Kafka UI, Prometheus y la API.
2. Crea el bucket S3 privado `ep5-kafka-deploy-<cuenta>` y sube `deploy/` y `scripts/`.
3. Ejecuta `terraform apply`, que crea la VPC, 2 subredes en 2 zonas, 3 security groups y las 4 EC2.

Cada EC2, en su primer arranque, instala Docker, descarga su configuración desde S3, escribe su `.env` y levanta sus contenedores.

```bash
./verificar.sh ec2
```
Lista las 4 EC2 con su tipo, estado, zona, IP privada (fija) e IP pública (cambia al apagar y encender).

```bash
./verificar.sh terraform
```
Lista los recursos que creó Terraform (`terraform state list`).

```bash
./verificar.sh estado
```
Muestra en cada EC2 si terminó su bootstrap (`BOOTSTRAP OK`) y el estado de sus contenedores Docker.

```bash
./verificar.sh urls
```
Entrega las direcciones de Grafana (:3000), Kafka UI (:8080), Prometheus (:9090) y la API (:8081).

```bash
terraform -chdir=infra output -raw grafana_password
```
```bash
terraform -chdir=infra output -raw kafka_ui_password
```
Muestran las contraseñas generadas por Terraform (usuario `admin`). No están en el repositorio.

```bash
./verificar.sh apagar
```
```bash
./verificar.sh encender
```
Detienen o inician las 4 EC2 sin perder datos. Las IP privadas no cambian, por eso Kafka sigue funcionando; Docker levanta solo los contenedores (`restart: unless-stopped`).

```bash
./verificar.sh destruir
```
Elimina toda la infraestructura (`terraform destroy`) y el bucket. Solo al final del curso.

## 4. Aplicación (en tu PC)

```bash
./verificar.sh subir-app
```
Compila la app con Maven (incluye las 25 pruebas), sube el jar a S3 y, en la EC2 app, construye la imagen Docker y levanta producer, Kafka Streams y la API. Muestra los últimos logs, incluida la restauración de los stores.

```bash
./verificar.sh subir-config
```
```bash
./verificar.sh sincronizar kafka-1
```
Suben al bucket los cambios de configuración de `deploy/` y `scripts/` y los aplican en la EC2 indicada (`kafka-1`, `kafka-2`, `app` o `monitoring`).

```bash
./verificar.sh pruebas
```
Ejecuta `mvn test` y lista cada prueba con su nombre y resultado. Debe dar 25 de 25 OK.

## 5. Pruebas de la evaluación con verificar.sh (en tu PC)

| Comando | Qué demuestra | Qué mirar en la salida |
|---|---|---|
| `./verificar.sh kafka` | clúster de 2 brokers y estado de ventas-raw | `[1, 2]`, `ReplicationFactor: 2`, `Isr: 1,2`, sin sub-replicadas |
| `./verificar.sh topicos` | creación idempotente y configuración de los topics | particiones, RF 2, `retention.ms`, `min.insync.replicas=1`, `unclean.leader.election.enable=false`, changelogs internos |
| `./verificar.sh flujo` | venta → ventas-raw → agregado | key, partición y offset de cada venta; agregado con `windowStart`, `windowEnd`, `cantidadVentas`, `montoTotal`, `ticketPromedio`; lag 0 |
| `./verificar.sh casos todos` | los 9 casos controlados | DLQ con tipo, origen y `rawValue`; `DUPLICADO` y `TARDIO_FUERA_DE_GRACIA` en descartadas; agregado de T-DEMO |
| `./verificar.sh resultados` | vuelve a mostrar los resultados de T-DEMO | igual que arriba, sin reenviar casos |
| `./verificar.sh inspeccionar` | topics, internos, consumer groups, offsets y lag | offset confirmado, offset final, lag y consumidor por partición |
| `./verificar.sh fallo` | tolerancia a la caída del broker 2 | ISR `1,2` → `1` → `1,2`; offset sigue creciendo; 0 errores |
| `./verificar.sh conciliar 30` | sin pérdidas ni dobles conteos | diferencia 0 en todas las ventanas |
| `./verificar.sh reinicio-streams` | recuperación de Kafka Streams sin estado local | `RESTAURACION fin ... registros restaurados`; reenvío = `DUPLICADO`; lag vuelve a 0 |
| `./verificar.sh escalar` | paralelismo con 2 instancias | 4 particiones repartidas 2 y 2 |
| `./verificar.sh monitoreo` | observabilidad | targets de Prometheus `up`, Grafana `ok`, Loki recibe de 4 hosts, conteos por motivo |
| `./verificar.sh api` | API REST de usuarios | respuestas de GET, POST, PUT, DELETE y 404 |
| `./verificar.sh evidencias` | todo lo anterior | guarda cada salida en `docs/evidencias/txt/` |

Casos individuales, para ir uno por uno:

```bash
./verificar.sh casos lista
```
```bash
./verificar.sh casos json-malformado
```
```bash
./verificar.sh casos duplicado
```
```bash
./verificar.sh casos tardio-fuera-de-gracia
```
Después de 90 segundos (60 s de ventana más 30 s de gracia), `./verificar.sh resultados` muestra dónde quedó cada uno.

## 6. Comandos manuales en kafka-1 (como root)

Primero se definen dos variables para acortar los comandos:

```bash
K="docker exec -i -e KAFKA_OPTS= kafka"
B=localhost:9092
```
`$K` ejecuta las herramientas de Kafka dentro del contenedor del broker. `-e KAFKA_OPTS=` vacía esa variable para la herramienta: el broker la usa para cargar el agente de métricas en el puerto 7071 y, si la herramienta la heredara, intentaría abrir el mismo puerto y fallaría. `-i` permite enviarle texto por la entrada estándar (para producir mensajes). `$B` es el broker local.

### 6.1 Clúster

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Contenedores de la EC2: `zookeeper`, `kafka` (ambos `healthy`) y `grafana-alloy`.

```bash
docker exec -e KAFKA_OPTS= zookeeper zookeeper-shell localhost:2181 ls /brokers/ids
```
Brokers registrados en ZooKeeper. Debe responder `[1, 2]`.

```bash
docker exec -e KAFKA_OPTS= zookeeper zookeeper-shell localhost:2181 get /controller
```
Qué broker es el controlador, el que elige líderes cuando algo falla.

### 6.2 Topics, réplicas e ISR

```bash
$K kafka-topics --bootstrap-server $B --list
```
Todos los topics: los 4 de negocio, los 3 changelog de Kafka Streams y los internos de Kafka (`__consumer_offsets`, `__transaction_state`).

```bash
$K kafka-topics --bootstrap-server $B --describe --topic ventas-raw
```
Por cada partición: `Leader` (broker que atiende), `Replicas` (dónde hay copia) e `Isr` (copias al día). Lo normal es `Replicas: 1,2  Isr: 1,2`.

```bash
$K kafka-configs --bootstrap-server $B --describe --entity-type topics --entity-name ventas-raw
```
Configuración propia del topic: `retention.ms`, `min.insync.replicas`, `cleanup.policy`, `unclean.leader.election.enable`.

```bash
$K kafka-topics --bootstrap-server $B --describe --under-replicated-partitions
```
Particiones con réplicas atrasadas. Vacío significa que todo está sincronizado.

Crear topics (lo hace `scripts/topicos.sh`; se muestra como referencia):

```bash
$K kafka-topics --bootstrap-server $B --create --if-not-exists --topic ventas-raw --partitions 4 --replication-factor 2 --config retention.ms=604800000
```
```bash
$K kafka-configs --bootstrap-server $B --alter --entity-type topics --entity-name ventas-raw --add-config min.insync.replicas=1,unclean.leader.election.enable=false,cleanup.policy=delete
```
`--if-not-exists` y `--add-config` hacen que se pueda repetir sin error (idempotente). Un `--replication-factor 3` fallaría con `InvalidReplicationFactorException`, porque solo hay 2 brokers.

```bash
BOOTSTRAP=localhost:9092 KAFKA_CMD="docker exec -i -e KAFKA_OPTS= kafka" bash /opt/ep5/scripts/topicos.sh todo
```
Ejecuta el script completo: crea, describe, muestra sub-replicadas y consumer groups.

### 6.3 Consumer groups, offsets y lag

```bash
$K kafka-consumer-groups --bootstrap-server $B --list
```
Grupos: `ventas-aggregator` (Kafka Streams) y `demo-logs-*` (lectores que llevan los mensajes a Loki).

```bash
$K kafka-consumer-groups --bootstrap-server $B --describe --group ventas-aggregator
```
Por partición: `CURRENT-OFFSET` (lo confirmado por Streams), `LOG-END-OFFSET` (último mensaje), `LAG` (diferencia) y `CONSUMER-ID` (instancia asignada). Lag 0 o cercano a 0 es lo normal.

```bash
$K kafka-get-offsets --bootstrap-server $B --topic ventas-raw
```
Último offset de cada partición (`topic:particion:offset`). Repetirlo muestra que crece mientras el productor escribe.

### 6.4 Leer mensajes

```bash
$K kafka-console-consumer --bootstrap-server $B --topic ventas-raw --max-messages 3 --property print.key=true --property print.partition=true --property print.offset=true
```
Ventas que van llegando, con su partición, offset y key (`tiendaId`).

```bash
$K kafka-console-consumer --bootstrap-server $B --topic ventas-agregadas --from-beginning --max-messages 4 --isolation-level read_committed --property print.key=true
```
Agregados por tienda y ventana. `read_committed` lee solo lo confirmado por las transacciones de `exactly_once_v2`.

```bash
$K kafka-console-consumer --bootstrap-server $B --topic ventas-raw-dlq --from-beginning --max-messages 3 --property print.key=true --property print.headers=true
```
Mensajes rechazados: `rawValue` (original intacto), `sourceTopic`, `sourcePartition`, `sourceOffset`, `errorType`, `error` y los headers `dlq.*`.

```bash
$K kafka-console-consumer --bootstrap-server $B --topic ventas-descartadas --from-beginning --max-messages 3
```
Duplicados y tardíos fuera de gracia, con `motivo`, `detalle`, ventana y stream time.

### 6.5 Producir mensajes a mano

JSON malformado (debe ir a la DLQ como `JSON_MALFORMADO`):

```bash
echo 'T-DEMO|{"eventId":"manual-roto","tiendaId":"T-DEMO",' | $K kafka-console-producer --bootstrap-server $B --topic ventas-raw --property parse.key=true --property key.separator='|'
```
`parse.key=true` y `key.separator='|'` hacen que lo que está antes de `|` sea la key del mensaje.

Venta válida; ejecutarla dos veces para probar el duplicado:

```bash
echo "T-DEMO|{\"eventId\":\"manual-dup-1\",\"tiendaId\":\"T-DEMO\",\"eventTime\":\"$(date -u +%FT%TZ)\",\"cantidad\":2,\"precioUnitario\":3000,\"total\":6000}" | $K kafka-console-producer --bootstrap-server $B --topic ventas-raw --property parse.key=true --property key.separator='|'
```
La primera vez se suma al agregado. La segunda va a `ventas-descartadas` como `DUPLICADO`. `$(date -u +%FT%TZ)` pone la hora actual como `eventTime`.

Total incorrecto (`REGLA_NEGOCIO`):

```bash
echo "T-DEMO|{\"eventId\":\"manual-total\",\"tiendaId\":\"T-DEMO\",\"eventTime\":\"$(date -u +%FT%TZ)\",\"cantidad\":2,\"precioUnitario\":3000,\"total\":5000}" | $K kafka-console-producer --bootstrap-server $B --topic ventas-raw --property parse.key=true --property key.separator='|'
```

Tardío fuera de gracia (`eventTime` de hace 5 minutos):

```bash
echo "T-DEMO|{\"eventId\":\"manual-tarde\",\"tiendaId\":\"T-DEMO\",\"eventTime\":\"$(date -u -d '-5 min' +%FT%TZ)\",\"cantidad\":1,\"precioUnitario\":1000,\"total\":1000}" | $K kafka-console-producer --bootstrap-server $B --topic ventas-raw --property parse.key=true --property key.separator='|'
```

Ver dónde quedó cada uno:

```bash
$K kafka-console-consumer --bootstrap-server $B --topic ventas-raw-dlq --from-beginning --timeout-ms 8000 | grep manual
```
```bash
$K kafka-console-consumer --bootstrap-server $B --topic ventas-descartadas --from-beginning --timeout-ms 8000 | grep manual
```
Esperado: `manual-roto` y `manual-total` en la DLQ; la segunda copia de `manual-dup-1` como `DUPLICADO` y `manual-tarde` como `TARDIO_FUERA_DE_GRACIA` en descartadas.

## 7. Prueba de caída de broker (kafka-1 y kafka-2)

| Paso | Dónde | Comando | Qué se ve |
|---|---|---|---|
| 1. Antes | kafka-1 | `$K kafka-topics --bootstrap-server $B --describe --topic ventas-raw` | `Isr: 1,2`, líderes repartidos |
| 2. Detener | kafka-2 | `docker stop kafka` | el broker 2 se apaga |
| 3. Durante | kafka-1 | `$K kafka-topics --bootstrap-server $B --describe --topic ventas-raw` | `Isr: 1`, todos los líderes en el broker 1 |
| 4. Durante | kafka-1 | `$K kafka-topics --bootstrap-server $B --describe --under-replicated-partitions \| wc -l` | más de 100 particiones sub-replicadas |
| 5. Durante | kafka-1 | `$K kafka-get-offsets --bootstrap-server $B --topic ventas-raw` (dos veces) | el offset sigue creciendo: se sigue escribiendo |
| 6. Levantar | kafka-2 | `docker start kafka` | el broker 2 vuelve |
| 7. Después | kafka-1 | `$K kafka-topics --bootstrap-server $B --describe --topic ventas-raw` | `Isr: 1,2` otra vez |

Por qué sigue funcionando: RF 2 deja una copia en el broker 1; `min.insync.replicas=1` permite escribir con `acks=all` teniendo una sola réplica al día; `transaction.state.log.min.isr=1` evita que se bloqueen las transacciones de `exactly_once_v2`. Riesgo: durante la caída cada escritura tiene una sola copia. Los líderes se reequilibran solos cada 300 s (`auto.leader.rebalance.enable`), o al instante con:

```bash
$K kafka-leader-election --bootstrap-server $B --election-type preferred --all-topic-partitions
```

## 8. Comandos en la EC2 app (como root)

```bash
cd /opt/ep5/app
```
Carpeta con el `docker-compose.yml` de la app.

```bash
docker compose ps
```
Contenedores: `producer`, `streams`, `api-usuarios` y `grafana-alloy`.

```bash
docker compose logs --tail 5 producer
```
Cada venta enviada: `ENVIADA key=T03 particion=3 offset=… eventId=…`.

```bash
docker compose logs streams | grep -E "Configuracion|Kafka Streams:"
```
Configuración efectiva (ventana 60 s, gracia 30 s, dedup 600 s) y estados (`REBALANCING -> RUNNING`).

```bash
docker compose logs --tail 30 streams | grep -E "DLQ|DESCARTE|TARDIO|AGREGADO"
```
Log contextual de cada decisión: `DLQ tipo=… origen=ventas-raw-P@offset`, `DESCARTE motivo=…`, `TARDIO_EN_GRACIA`, `AGREGADO tienda=…`.

```bash
docker compose run --rm casos casos lista
```
```bash
docker compose run --rm casos casos todos
```
Lista y ejecuta los 9 casos controlados sobre la tienda T-DEMO. Cada envío muestra partición, offset y resultado esperado.

```bash
docker compose run --rm casos conciliar 20
```
Compara por ventana las ventas válidas únicas de `ventas-raw` con la suma de `ventas-agregadas`. Debe dar diferencia 0.

Reinicio de Kafka Streams sin estado local:

```bash
docker compose rm -sf streams && docker compose up -d streams
```
Borra el contenedor (y su RocksDB local) y crea uno nuevo.

```bash
docker compose logs -f streams | grep -E "RESTAURACION|Kafka Streams:"
```
Muestra cómo reconstruye los 3 stores desde sus changelog en Kafka y pasa a `RUNNING`. Salir con Ctrl+C.

Escalar a 2 instancias y volver a 1:

```bash
docker compose up -d --scale streams=2 streams
```
```bash
docker compose up -d --scale streams=1 streams
```
Con el mismo `application.id`, Kafka reparte las 4 particiones entre las instancias (se ve con `kafka-consumer-groups --describe` en kafka-1).

```bash
curl -s localhost:8081/api/usuarios
```
Prueba local de la API Flask.

## 9. Comandos en la EC2 monitoring (como root)

```bash
cd /opt/ep5/monitoring && docker compose ps
```
Contenedores: `prometheus`, `grafana`, `loki`, `kafka-exporter`, `kafka-ui`, `grafana-alloy`.

```bash
curl -s localhost:9090/api/v1/targets | python3 -m json.tool | grep -E '"job"|"health"'
```
Estado de cada objetivo de Prometheus (brokers, ZooKeeper, kafka-exporter, producer, Streams).

```bash
curl -s localhost:3100/loki/api/v1/label/host/values
```
Hosts que envían logs a Loki: deben aparecer los 4.

## 10. Comandos en kafka-2 (como root)

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Broker 2 y los 4 lectores (`lector-ventas-raw`, `-agregadas`, `-dlq`, `-descartadas`).

```bash
docker logs --tail 3 lector-ventas-dlq
```
Últimos mensajes leídos de la DLQ (lo mismo que llega a Loki y se ve en Grafana).

```bash
docker stop kafka
```
```bash
docker start kafka
```
Detener y levantar el broker 2 (prueba de falla, sección 7).

## 11. Plan B local (sin AWS, en tu PC con Docker Desktop)

```bash
cd app && mvn -B package && cd ..
```
```bash
docker compose -f local/docker-compose.local.yaml up -d --build
```
```bash
docker compose -f local/docker-compose.local.yaml run --rm casos casos todos
```
```bash
docker compose -f local/docker-compose.local.yaml down -v
```
Levanta en un solo equipo ZooKeeper, 2 brokers, los topics (con el mismo `topicos.sh`), Kafka UI (http://localhost:8080), productor, Kafka Streams y la API; ejecuta los casos y al final borra todo.

## 12. Resumen para la defensa

1. `./verificar.sh estado` y `./verificar.sh kafka`: el clúster está sano.
2. `./verificar.sh topicos`: particiones, RF 2 y configuración justificada.
3. `./verificar.sh flujo`: venta → agregado con ventana por tiempo de evento.
4. `./verificar.sh casos todos`: DLQ, duplicado y tardíos (inválido ≠ tardío).
5. `./verificar.sh fallo` y `./verificar.sh conciliar 30`: caída de broker sin pérdidas.
6. `./verificar.sh reinicio-streams`: el estado vive en Kafka (changelog).
7. `./verificar.sh escalar`, `./verificar.sh monitoreo` y `./verificar.sh pruebas`.
8. Cerrar con los límites: ZooKeeper es un punto único de falla; con 2 brokers se tolera solo una caída.
