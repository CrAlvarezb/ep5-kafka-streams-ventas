# EP5 · Streaming resiliente de ventas con Kafka Streams

DSY1107 · Desarrollo Cloud Native I · Evaluación Parcial N° 5

Flujo de ventas en tiempo real sobre un clúster Kafka de **2 brokers + 1 ZooKeeper** desplegado en
**AWS Academy (4 EC2)** con Terraform:

```
ProducerApp ──> ventas-raw ──> Kafka Streams (Java) ──> ventas-agregadas
                                  │   │
                                  │   └─(duplicado / tardío fuera de gracia)──> ventas-descartadas
                                  └─(JSON roto / campo inválido / regla de negocio)──> ventas-raw-dlq
```

- Diagrama de flujo: [docs/arquitectura.mmd](docs/arquitectura.mmd) · Despliegue: [docs/infraestructura.mmd](docs/infraestructura.mmd)
- Informe y matriz de escenarios: [docs/INFORME.md](docs/INFORME.md)
- Guía para ejecutar y defender cada prueba: [docs/GUIA_PRUEBAS.md](docs/GUIA_PRUEBAS.md)
- Comandos explicados: [docs/COMANDOS.md](docs/COMANDOS.md) · Bitácora de trabajo: [docs/BITACORA.md](docs/BITACORA.md)
- Documentos Word: [documentos/](documentos/)
- Evidencias (salida real de cada comando): [docs/evidencias/txt/](docs/evidencias/txt/)

---

## 1. Estructura del repositorio

| Ruta | Contenido |
|---|---|
| `app/` | Aplicación Java 17 (Maven): `producer`, `streams`, `casos`, `conciliar` en un solo jar. Pruebas JUnit 5 + `TopologyTestDriver`. |
| `deploy/kafka-1/` | Docker Compose: ZooKeeper + broker 1 + Alloy |
| `deploy/kafka-2/` | Docker Compose: broker 2 + 4 lectores de topics (kcat) + Alloy |
| `deploy/app/` | Docker Compose: producer + Kafka Streams + `api-usuarios` (Flask) + Alloy |
| `deploy/monitoring/` | Docker Compose: Prometheus + Grafana (dashboard provisionado) + Loki + kafka-exporter + Kafka UI + Alloy |
| `deploy/common/` | Configuración común: Alloy (logs → Loki) y JMX exporter |
| `scripts/topicos.sh` | Crea (idempotente) e inspecciona topics, configuración, ISR, internos y consumer groups |
| `infra/` | Terraform: VPC, 2 subredes en 2 AZ, 3 security groups, 4 EC2 con bootstrap |
| `local/` | Plan B: todo el sistema en un solo equipo con Docker Compose |
| `verificar.sh` | Operación y pruebas de todos los escenarios en AWS (por SSM, sin SSH) |

## 2. Prerrequisitos

- Cuenta de **AWS Academy Learner Lab** con el lab iniciado (punto verde).
- En tu equipo: **AWS CLI v2**, **Terraform ≥ 1.6**, **Java 17**, **Maven 3.9**, **bash** (Linux/macOS o Git Bash en Windows), `curl`.
- Plan B local: Docker Desktop.

### Credenciales (nunca en el repositorio)

Copia las credenciales del lab **en tu equipo**, no en el repo (cambian en cada sesión del lab):

```bash
aws configure                       # Access Key, Secret Key, región us-east-1, formato json
aws configure set aws_session_token <TOKEN_DE_AWS_DETAILS>
aws sts get-caller-identity         # debe responder assumed-role/voclabs/...
```

El repo no contiene `.env` reales, claves ni contraseñas: `.gitignore` excluye `*.env`, `*.tfstate`,
`*.tfvars`, `*.pem`. Las contraseñas de Grafana y Kafka UI las genera Terraform (`random_password`) y
se leen con `terraform -chdir=infra output -raw grafana_password`. Hay ejemplos en
`app/app.env.example`, `deploy/.env.example` e `infra/terraform.tfvars.example`.

## 3. Puesta en marcha (AWS, desde cero)

```bash
./verificar.sh desplegar      # bucket S3 + terraform apply (VPC, SG, 4 EC2). Bootstrap ~5 min
./verificar.sh subir-app      # mvn package (con pruebas) + sube y levanta producer + Kafka Streams
./verificar.sh estado         # BOOTSTRAP OK y contenedores de cada EC2
./verificar.sh urls           # Grafana, Kafka UI, Prometheus y API de usuarios
./verificar.sh evidencias     # ejecuta todos los escenarios y guarda la salida en docs/evidencias/txt
```

Sin editar código. Cada EC2 descarga su `deploy/<rol>` desde S3 al arrancar (rol `LabInstanceProfile`),
escribe su `.env` con las IP privadas fijas y levanta sus contenedores (`restart: unless-stopped`):
al apagar y encender las EC2 todo vuelve solo.

| EC2 | Tipo | IP privada fija | Servicios |
|---|---|---|---|
| kafka-1 | t3.small | 10.20.1.10 (us-east-1a) | ZooKeeper :2181, broker 1 :9092, JMX :7071/:7072 |
| kafka-2 | t3.small | 10.20.2.10 (us-east-1b) | broker 2 :9092, lectores de topics |
| app | t3.small | 10.20.2.20 (us-east-1b) | producer (:9406), Kafka Streams (:9407-9408), api-usuarios :8081 |
| monitoring | t3.medium | 10.20.2.30 (us-east-1b) | Grafana :3000, Kafka UI :8080, Prometheus :9090, Loki :3100 |

Seguridad de red: el tráfico Kafka/ZooKeeper/métricas/logs solo se permite **dentro de la VPC**;
Grafana, Kafka UI, Prometheus y la API solo **desde tu IP** (/32). No se abre el puerto 22: la operación
es por **SSM**.

> Nota Learner Lab: una política de la organización niega `s3:GetBucketObjectLockConfiguration`, que el
> recurso `aws_s3_bucket` de Terraform necesita; por eso el bucket lo crea `verificar.sh` con AWS CLI y
> Terraform lo recibe como variable.

### Comandos de `verificar.sh`

| Comando | Qué hace |
|---|---|
| `desplegar` / `destruir` | crea / elimina toda la infraestructura |
| `ec2`, `terraform`, `estado`, `urls` | inventario y salud |
| `subir-app`, `subir-config`, `sincronizar <rol>` | despliegue de app y configuración |
| `kafka`, `topicos`, `inspeccionar` | brokers, topics (particiones, RF, retención, min ISR), internos, grupos, offsets, lag |
| `app`, `lectores`, `flujo` | estado de la app y mensajes de entrada/salida |
| `casos [caso\|todos]`, `resultados` | casos controlados y su resultado en DLQ / descartadas / agregados |
| `fallo` | detiene el broker 2 con producción: ISR antes/durante/después |
| `conciliar [n]` | raw vs agregados por ventana: sin pérdidas ni dobles conteos |
| `reinicio-streams` | elimina Kafka Streams (sin estado local) y comprueba restauración y dedup |
| `escalar` | 2 instancias de Streams y reparto de particiones |
| `monitoreo` | targets de Prometheus, Grafana, Loki y conteos por motivo |
| `api` | prueba la API REST de usuarios (GET/POST/PUT/DELETE) |
| `pruebas` | `mvn test` con detalle por prueba |
| `apagar` / `encender` | detiene / inicia las 4 EC2 sin perder datos |

### Plan B local (sin AWS)

```bash
cd app && mvn -B package && cd ..
docker compose -f local/docker-compose.local.yaml up -d --build
docker compose -f local/docker-compose.local.yaml run --rm casos casos todos
docker compose -f local/docker-compose.local.yaml run --rm casos conciliar 10
docker compose -f local/docker-compose.local.yaml down -v
```

## 4. Modelo de eventos, topics y claves

**Venta** (`ventas-raw`, JSON tipado → `cl.duoc.ventas.modelo.Venta`):

```json
{"eventId":"5e0c…","tiendaId":"T03","tiendaNombre":"Las Condes","eventTime":"2026-11-22T17:40:12.345Z",
 "clienteId":"cliente-0042","producto":"Audifonos","categoria":"tecnologia","canal":"presencial",
 "medioPago":"debito","cantidad":2,"precioUnitario":24990,"total":49980}
```

Montos en **CLP enteros** (sin errores de punto flotante al validar `total = cantidad × precioUnitario`).

| Topic | Part. | RF | Key | Retención | Justificación |
|---|---|---|---|---|---|
| `ventas-raw` | 4 | 2 | `tiendaId` | 7 días | 4 particiones = hasta 4 instancias de Streams en paralelo; 7 días para reprocesar |
| `ventas-agregadas` | 2 | 2 | `tiendaId` | 30 días | salida pequeña (1 registro por tienda y minuto) |
| `ventas-raw-dlq` | 4 | 2 | key original | 14 días | tiempo para corregir y reinyectar |
| `ventas-descartadas` | 2 | 2 | `tiendaId` | 14 días | auditoría de duplicados y tardíos |
| `ventas-aggregator-*-changelog` (3) | 4 | 2 | — | compactado | estado de dedup, ventanas y suppress |

Todos: `min.insync.replicas=1`, `unclean.leader.election.enable=false`, `cleanup.policy=delete`
(changelogs compactados), `auto.create.topics.enable=false` (solo los crea `scripts/topicos.sh`).

**Por qué key = `tiendaId`**: la agregación es por tienda; con esa key todas las ventas de una tienda
llegan a la misma partición y tarea de Streams (orden por tienda + estado local), **sin repartition**
(las etapas usan `processValues`, que no cambia la key). El validador exige `key == tiendaId`.
Costo: el reparto depende del hash de 8 tiendas en 4 particiones (3/1/2/2), ver límites.

**Por qué RF = 2 y no 3**: con 2 brokers solo hay 2 lugares distintos para las réplicas; RF 3 es
imposible (`InvalidReplicationFactorException`). `Config` rechaza `REPLICATION_FACTOR > 2`.

**Por qué `min.insync.replicas=1`**: con un broker caído el ISR baja a 1 y el productor (`acks=all`)
sigue escribiendo (se privilegia disponibilidad). Con `min.insync.replicas=2` la producción se
detendría (`NotEnoughReplicasException`). En los brokers además
`transaction.state.log.min.isr=1`: con el valor por defecto (2) las transacciones de
`exactly_once_v2` se bloquean si cae un broker.

## 5. Procesamiento (Kafka Streams)

Configuración (`app/app.env.example`, validada por `Config.java` al iniciar):

| Variable | Valor | Por qué |
|---|---|---|
| `VENTANA_SEGUNDOS` | 60 | un resumen por tienda y minuto |
| `GRACIA_SEGUNDOS` | 30 | absorbe reintentos y cajas con red intermitente; costo: el agregado sale 30 s después del fin de la ventana |
| `DEDUP_RETENCION_SEGUNDOS` | 600 | ≥ ventana + gracia (90 s): un duplicado más tardío ya sería tardío y se descartaría igual. `Config` rechaza valores menores |
| `FUTURO_MAX_SEGUNDOS` | 300 | un `eventTime` más de 5 min en el futuro adelantaría el stream time y cerraría ventanas antes de tiempo |
| `processing.guarantee` | `exactly_once_v2` | ver §6 |
| `replication.factor` | 2 | topics internos con la misma tolerancia que los de negocio |

1. **Tiempo de evento**: `EventTimeExtractor` usa `eventTime` como timestamp. Si el valor es inválido o
   el `eventTime` es futuro, usa el timestamp del registro: un dato basura nunca adelanta el stream time.
2. **Validación** (`ValidadorVenta`), en orden:
   - `JSON_MALFORMADO`: no parsea o no es objeto.
   - `CAMPO_INVALIDO`: falta `eventId`/`tiendaId`/`eventTime`/`cantidad`/`precioUnitario`/`total`, `eventTime` no ISO-8601, números no enteros o ≤ 0.
   - `REGLA_NEGOCIO`: `total ≠ cantidad × precioUnitario`, `eventTime` futuro, key ≠ `tiendaId`.
   
   El inválido va a `ventas-raw-dlq` como `RegistroDlq`: `rawValue` (original intacto), `originalKey`,
   `sourceTopic`, `sourcePartition`, `sourceOffset`, `eventId`, `errorType`, `error`, `failedAt`
   (+ headers `dlq.*`), con log `DLQ tipo=… motivo=… origen=ventas-raw-P@offset key=… eventId=…`.
   **Nunca se lanza excepción**: el consumidor sigue.
3. **Deduplicación** (`DedupTiempoProcessor`, store `dedup-event-id`, RocksDB + changelog): si el
   `eventId` ya fue aceptado → `ventas-descartadas` con `DUPLICADO` (no suma). El `eventId` se guarda
   solo cuando la venta se acepta.
4. **Tardíos**: con `fin de ventana + gracia ≤ stream time` → `ventas-descartadas` con
   `TARDIO_FUERA_DE_GRACIA` (+ log `DESCARTE`); si su ventana sigue abierta → se suma a su ventana
   original (log `TARDIO_EN_GRACIA`). **Tardío ≠ inválido**: el inválido es un dato que no se puede usar
   (DLQ, para corregirlo); el tardío es un dato correcto que llegó tarde (se audita, no se corrige).
5. **Agregación**: ventana tumbling por tienda (`TimeWindows.ofSizeAndGrace(60 s, 30 s)`) +
   `suppress(untilWindowCloses)`: un solo resultado final por ventana, sin parciales.
   `ventas-agregadas`: `tiendaId`, `windowStart`, `windowEnd`, `cantidadVentas`, `unidades`,
   `montoTotal`, `ticketPromedio` (= monto / ventas), `primerEvento`, `ultimoEvento`, `publicadoEn`.

## 6. `exactly_once_v2` frente a la deduplicación de negocio

| | `exactly_once_v2` | Deduplicación por `eventId` |
|---|---|---|
| Evita | procesar dos veces el **mismo registro** (mismo offset) tras un fallo/reinicio; escrituras parciales | contar dos veces la **misma venta** enviada en **dos registros distintos** |
| Cómo | una transacción une offsets consumidos + cambios de stores (changelog) + escrituras de salida | store `dedup-event-id` con changelog, consultado antes de agregar |
| No garantiza | que el productor no reenvíe una venta: el reenvío tiene **otro offset** y para Kafka es otro mensaje | duplicados separados por más de 600 s |

La idempotencia del productor (`enable.idempotence=true`) solo evita duplicados por **reintentos de
red del mismo cliente**; si la aplicación (o el usuario) vuelve a enviar la venta, Kafka la recibe como
un registro nuevo. Por eso se necesitan las dos cosas.

## 7. Casos controlados (`./verificar.sh casos todos`)

| Caso | Envío | Resultado esperado |
|---|---|---|
| `valida` | venta correcta $5.000 | se suma al agregado de T-DEMO |
| `json-malformado` | JSON truncado | DLQ `JSON_MALFORMADO`, `rawValue` intacto |
| `campo-faltante` | sin `eventTime` | DLQ `CAMPO_INVALIDO` |
| `campo-invalido` | `cantidad: "dos"` | DLQ `CAMPO_INVALIDO` |
| `total-incorrecto` | 2 × 5000 ≠ 9999 | DLQ `REGLA_NEGOCIO` |
| `fecha-futura` | `eventTime` +2 h | DLQ `REGLA_NEGOCIO` (no adelanta el stream time) |
| `duplicado` | misma venta 2 veces | 1.ª suma; 2.ª → `ventas-descartadas` `DUPLICADO` |
| `tardio-en-gracia` | ancla + venta 15 s anterior | se suma a su ventana original |
| `tardio-fuera-de-gracia` | ancla + venta 3 min anterior | `ventas-descartadas` `TARDIO_FUERA_DE_GRACIA`, no DLQ |

Resultados obtenidos y evidencias: [docs/INFORME.md](docs/INFORME.md).

## 8. Pruebas automatizadas

```bash
cd app && mvn -B test        # o ./verificar.sh pruebas
```

25 pruebas (JUnit 5): `TopologiaTest` ejecuta la topología real con `TopologyTestDriver` (agregación y
cierre de ventana, DLQ con original/key/topic/partición/offset/motivo y continuidad, errores de campo y
negocio, duplicados, tardío en gracia, tardío fuera de gracia, fecha futura que no adelanta el stream
time). `ValidadorVentaTest`: 14 rechazos parametrizados, key ≠ tienda, DTO tipado, evento antiguo
válido y configuración inválida.

## 9. Observabilidad

- **Kafka UI** (`:8080`): brokers, topics, particiones, réplicas, mensajes, consumer groups y lag.
- **Grafana** (`:3000`, dashboard *EP5 - Kafka y streaming de ventas*): brokers activos, particiones
  sub-replicadas, ISR por partición, lag por partición, mensajes/s por topic, instancias de Streams,
  ZooKeeper; fila **Negocio y resiliencia** desde Loki: rechazos DLQ por tipo, descartes por motivo,
  tardíos en gracia, agregados publicados y registros restaurados; heap por proceso.
- **Prometheus** (`:9090`): JMX exporter de brokers, ZooKeeper, producer y Streams + kafka-exporter.
- **Loki**: logs de todos los contenedores (Alloy) y de los 4 lectores de topics.

| Métrica | Qué indica | Acción |
|---|---|---|
| Lag de `ventas-aggregator` | mensajes aún no procesados | si crece sostenidamente: escalar Streams (máx. 4) o revisar errores |
| Sub-replicadas / ISR | réplicas fuera de sincronía | revisar el broker caído; sin redundancia hasta que vuelva |
| Rechazos DLQ por tipo | calidad de los datos de origen | corregir el origen y reinyectar desde la DLQ |
| `TARDIO_FUERA_DE_GRACIA` | atrasos mayores que la gracia | evaluar subir la gracia (más latencia) |
| ZooKeeper caído | SPOF | ver recuperación |

Métricas de negocio **no instrumentadas** como métricas Prometheus propias: los conteos por motivo
salen de los logs (Loki), no de contadores Micrometer.

## 10. Límites conocidos

1. **ZooKeeper es un punto único de falla** (1 nodo en kafka-1). Si cae, los brokers siguen sirviendo
   con los líderes actuales, pero no hay elección de controlador ni de líderes, ni cambios de ISR ni
   creación de topics; si además cae un broker, sus particiones quedan sin líder. Perder kafka-1 es
   perder ZooKeeper y el broker 1 a la vez.
2. **Dos brokers**: RF máximo 2 y tolerancia a **un** broker caído; durante la caída cada escritura
   tiene una sola copia (`min.insync.replicas=1`).
3. **Reparto por hash**: 8 tiendas en 4 particiones quedan 3/1/2/2.
4. **Deduplicación** limitada a 600 s y a la misma tienda (la key).
5. **Stream time por partición**: una partición sin tráfico no cierra sus ventanas hasta recibir otra venta.
6. **Latencia del agregado**: fin de ventana + 30 s de gracia.
7. **Seguridad de laboratorio**: Kafka sin TLS ni ACL (solo accesible dentro de la VPC); Grafana,
   Kafka UI, Prometheus y la API por HTTP, solo desde la IP del operador; contraseñas en el `.env`
   de la EC2 monitoring (user data).

**Mejora priorizada**: 3 brokers con KRaft (sin ZooKeeper), RF 3 y `min.insync.replicas=2`.

## 11. Recuperación

| Situación | Pasos |
|---|---|
| Broker caído | `docker start kafka` en su EC2 (o `./verificar.sh encender`); esperar ISR completo (`./verificar.sh kafka`); opcional `kafka-leader-election --election-type preferred --all-topic-partitions` |
| Kafka Streams caído | Docker lo reinicia (`restart: unless-stopped`; si entra en `ERROR` el proceso termina para reiniciarse). Retoma desde los offsets confirmados y reconstruye los stores desde los changelog |
| Estado local corrupto | `docker compose rm -sf streams && docker compose up -d streams` (restaura desde changelog) |
| Reinyectar desde la DLQ | leer `ventas-raw-dlq`, corregir `rawValue` y producirlo en `ventas-raw` con su key original; el `eventId` evita doble conteo si ya se había aceptado |
| Reprocesar todo | detener Streams, `kafka-streams-application-reset --application-id ventas-aggregator --input-topics ventas-raw`, borrar estado y levantar |
| ZooKeeper caído | `docker start zookeeper` en kafka-1; los brokers se reconectan solos; verificar `ls /brokers/ids` = `[1, 2]` |
| Learner Lab terminó la sesión | las EC2 se detienen; al iniciar el lab: `./verificar.sh encender` (IP privadas fijas, contenedores vuelven solos) |

## 12. API de usuarios (Flask)

`deploy/app/api-usuarios/` contiene la API REST entregada por el curso (CRUD de usuarios en memoria),
servida con gunicorn en la EC2 app (`:8081`, 1 worker porque los datos viven en memoria; `debug`
desactivado). No participa del flujo Kafka. Prueba: `./verificar.sh api`.
