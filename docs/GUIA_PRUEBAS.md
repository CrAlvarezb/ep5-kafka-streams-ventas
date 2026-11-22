# Guía de pruebas y defensa (EP5 / EP6)

Cómo ejecutar cada prueba **en vivo**, qué mostrar en pantalla y qué explicar para demostrar dominio.
Todo se ejecuta desde la raíz del repo en **Git Bash** (con las credenciales del Learner Lab cargadas).

## 0. Antes de presentar (5 min)

```bash
aws sts get-caller-identity        # credenciales vigentes (cambian en cada sesión del lab)
./verificar.sh encender            # si las EC2 estaban apagadas (las IP públicas cambian)
./verificar.sh estado              # BOOTSTRAP OK y contenedores Up en las 4 EC2
./verificar.sh urls                # abrir Grafana y Kafka UI en el navegador
terraform -chdir=infra output -raw grafana_password    # usuario admin (no mostrar en pantalla)
terraform -chdir=infra output -raw kafka_ui_password
```

Ten abiertas 3 ventanas: **terminal**, **Kafka UI** (Topics / Consumers) y **Grafana** (dashboard
*EP5 - Kafka y streaming de ventas*). No muestres el contenido de `~/.aws/credentials` ni las claves.

---

## 1. Arquitectura (2 min): "qué hay en cada máquina y por qué"

**Mostrar**: `docs/arquitectura.mmd` y `./verificar.sh ec2`.

**Explicar**:
- 4 EC2: kafka-1 (ZooKeeper + broker 1), kafka-2 (broker 2), app (productor + Kafka Streams + API),
  monitoring (Prometheus, Grafana, Loki, Kafka UI). IP privadas **fijas**: Kafka anuncia esas IP
  (`advertised.listeners`), así sobreviven a apagar/encender.
- Kafka solo es accesible **dentro de la VPC**; desde fuera solo Grafana/Kafka UI/API y **solo desde
  mi IP**. No hay puerto 22: se opera por **SSM**.
- Flujo: `ventas-raw → Kafka Streams → ventas-agregadas`; inválidos → `ventas-raw-dlq`; duplicados y
  tardíos → `ventas-descartadas`.

## 2. Topics, particiones y replicación (2 min)

```bash
./verificar.sh kafka        # brokers [1, 2], ISR 1,2
./verificar.sh topicos      # crea (idempotente) y describe con su configuración
```

**Qué señalar en la salida**: `PartitionCount: 4  ReplicationFactor: 2`, `Isr: 1,2`, y la
configuración `min.insync.replicas=1`, `retention.ms`, `unclean.leader.election.enable=false`.

**Explicar**:
- **RF 2, no 3**: con 2 brokers solo hay 2 lugares distintos para las réplicas; RF 3 falla con
  `InvalidReplicationFactorException`.
- **min.insync.replicas=1**: si cae un broker, el productor con `acks=all` sigue escribiendo
  (disponibilidad). Con 2 se detendría con `NotEnoughReplicas` (consistencia).
- **unclean.leader.election=false**: una réplica atrasada nunca pasa a líder → no se pierden datos
  confirmados.
- **Key = tiendaId**: todas las ventas de una tienda van a la misma partición → orden por tienda y
  agregación sin repartition. **4 particiones** = hasta 4 instancias de Streams en paralelo.
- **Retenciones**: raw 7 d (reprocesar), agregadas 30 d, DLQ 14 d (corregir y reinyectar).

## 3. Flujo normal (2 min)

```bash
./verificar.sh flujo
```

**Mostrar**: una venta en `ventas-raw` con `particion / offset / key=T0x` y un agregado en
`ventas-agregadas` con `windowStart`, `windowEnd`, `cantidadVentas`, `montoTotal`, `ticketPromedio`;
lag 0 del grupo `ventas-aggregator`. En Kafka UI: *Topics → ventas-agregadas → Messages*.

**Explicar**:
- **Tiempo de evento**: el timestamp de cada registro es su `eventTime` (TimestampExtractor), no la
  hora de llegada. El **stream time** de cada partición es el mayor `eventTime` visto.
- **Ventana tumbling 60 s + gracia 30 s**: un resumen por tienda y minuto; la gracia acepta ventas que
  llegan hasta 30 s tarde. `suppress` publica **solo al cierre** (fin + 30 s): un resultado final, sin
  parciales. Por eso el agregado aparece ~30 s después del fin del minuto.
- `ticketPromedio = montoTotal / cantidadVentas`.

## 4. Casos controlados (5 min) — el corazón de la evaluación

```bash
./verificar.sh casos todos        # envía los 9 casos, espera 100 s y muestra resultados
```
(También uno por uno: `./verificar.sh casos json-malformado`, `... duplicado`, etc. y luego
`./verificar.sh resultados`.)

| Caso | Qué mostrar | Qué decir |
|---|---|---|
| JSON malformado | DLQ `JSON_MALFORMADO`, `rawValue` truncado tal cual | No se puede parsear → DLQ con el **original intacto**, la **key**, el **origen exacto** (`ventas-raw-P@offset`) y el **motivo**. El consumidor **no se detiene** (log `DLQ tipo=...`, contenedor sigue Up) |
| Campo ausente / inválido | DLQ `CAMPO_INVALIDO` | Falta `eventTime` / `cantidad:"dos"` → error de estructura |
| Total incorrecto | DLQ `REGLA_NEGOCIO` "9999 != 2 x 5000" | JSON válido pero viola la regla de negocio. Montos en CLP enteros para comparar exacto |
| Fecha futura | DLQ `REGLA_NEGOCIO` | Si se aceptara, adelantaría el stream time y **cerraría ventanas antes de tiempo** |
| Duplicado | 1.ª suma, 2.ª `DUPLICADO` "ya aceptado en ventas-raw-1@117" | Ver §5 |
| Tardío en gracia | log `TARDIO_EN_GRACIA atraso=15s` y se suma | Su ventana aún no cierra (fin + gracia > stream time) |
| Tardío fuera de gracia | `TARDIO_FUERA_DE_GRACIA` en `ventas-descartadas`, **no** en DLQ | La ventana ya cerró. **Tardío ≠ inválido**: el dato es correcto, solo llegó tarde → se audita, no se corrige |

**Cuenta del agregado de T-DEMO** (para mostrar que el duplicado no suma): válida 5.000 + duplicado
10.000 (una vez) + 2 anclas 1.000 + tardío en gracia 7.000 = **5 ventas / $24.000**. Sin
deduplicación serían 6 / $34.000.

En Kafka UI: *ventas-raw-dlq → Messages* → abrir un mensaje y mostrar `sourceTopic`,
`sourcePartition`, `sourceOffset`, `originalKey`, `rawValue`, `errorType`, `error`, `failedAt` y los
**headers** `dlq.*`. En Grafana: fila *Negocio y resiliencia* (rechazos por tipo, descartes por motivo).

**¿Cómo se diferencia un descarte por tardío de un JSON inválido?** Distinto topic
(`ventas-descartadas` vs `ventas-raw-dlq`), distinto campo (`motivo` vs `errorType`), distinto log
(`DESCARTE motivo=TARDIO_FUERA_DE_GRACIA` vs `DLQ tipo=JSON_MALFORMADO`) y distinto panel en Grafana.

## 5. exactly_once_v2 vs. deduplicación (2 min) — pregunta segura

**Explicar con el caso duplicado en pantalla**:
- `processing.guarantee=exactly_once_v2`: en **una transacción** se confirman juntos los offsets
  leídos, los cambios de los state stores (changelog) y lo escrito en los topics de salida. Si Streams
  se cae a mitad, al reiniciar **no vuelve a contar el mismo offset**.
- Pero el productor envió **dos registros distintos** (offsets 117 y 118) con el mismo `eventId`. Para
  Kafka son dos mensajes legítimos: EOS los procesa exactamente una vez **cada uno**. Detectar que son
  la *misma venta de negocio* es responsabilidad de la aplicación → store `dedup-event-id`.
- La **idempotencia del productor** (`enable.idempotence`) solo evita duplicados por reintentos de red
  del mismo cliente, no reenvíos de la aplicación.
- Retención del dedup = 600 s ≥ ventana + gracia (90 s): un duplicado más tardío ya sería tardío.

## 6. Caída de un broker (4 min)

```bash
./verificar.sh fallo
```

**Señalar en la salida**:
1. **Antes**: `Isr: 1,2`, 0 sub-replicadas, lag 0.
2. **Durante** (broker 2 detenido): `Isr: 1`, **todos los líderes en el broker 1**, ~124 particiones
   sub-replicadas; el **offset sigue creciendo** (el productor escribe) y **0 errores** en productor y
   Streams. En Grafana: pico en *Particiones sub-replicadas* y *Brokers activos = 1*.
3. **Después**: el broker 2 se pone al día (replica lo que se perdió) y vuelve a `Isr: 1,2`, 0
   sub-replicadas. Los líderes se reequilibran con `auto.leader.rebalance.enable` (cada 300 s).

```bash
./verificar.sh conciliar 30     # diferencia 0 por ventana, incluidas las de la caída
```

**Explicar**: funciona porque RF 2 + `min.insync.replicas=1` + `transaction.state.log.min.isr=1`
(si no, las transacciones de EOS se bloquearían). **Riesgo**: durante la caída cada escritura tiene
**una sola copia**; si cae también el broker 1 antes de que el 2 se ponga al día, se pierde ese tramo.

## 7. Reinicio de Kafka Streams sin estado (3 min)

```bash
./verificar.sh reinicio-streams
```

**Señalar**: (1) offsets confirmados antes; (2) se envía `eventId=reinicio-HHMMSS`; (3) se **elimina**
el contenedor (se pierde RocksDB local) → el nuevo registra `RESTAURACION fin store=... registros
restaurados=N` para los 3 stores (~5.000 registros) y pasa a RUNNING; (4) se reenvía la misma venta →
**DUPLICADO**: el store de dedup se reconstruyó desde su changelog; (5) lag vuelve a 0.

**Explicar**: el estado vive en Kafka (topics `*-changelog`, compactados, RF 2), no en el disco del
contenedor. El consumidor retoma desde los **offsets confirmados** en la misma transacción que el
estado → nada se pierde ni se cuenta dos veces.

## 8. Escalamiento y consumer groups (2 min)

```bash
./verificar.sh escalar          # 2 instancias, mismo application.id → particiones 2 y 2
./verificar.sh inspeccionar     # offset confirmado, offset final, lag y consumidor por partición
```

**Explicar**: mismo `application.id` = mismo consumer group; Kafka reparte las 4 particiones. Máximo
útil = 4 instancias (una por partición). **Lag** = offset final − offset confirmado: si crece de forma
sostenida, escalar o revisar errores.

## 9. Observabilidad (2 min)

```bash
./verificar.sh monitoreo
```

Grafana: brokers activos, sub-replicadas, ISR por partición, lag por partición, mensajes/s, ZooKeeper,
y la fila de negocio desde **Loki** (logs). Decir explícitamente: las métricas de negocio (rechazos por
motivo) **salen de logs**, no son contadores Prometheus propios.

## 10. Pruebas automatizadas (1 min)

```bash
./verificar.sh pruebas          # 25/25 OK
```

`TopologyTestDriver` ejecuta **la topología real sin broker**: se controla el tiempo (timestamps) y
se prueban agregación, DLQ, duplicados, tardío dentro/fuera de gracia y fecha futura.

## 11. Límites (1 min) — decirlo antes de que lo pregunten

1. **ZooKeeper es un punto único de falla** (1 nodo, y en la misma EC2 que el broker 1). Si cae: los
   brokers siguen sirviendo con los líderes actuales, pero no hay elección de líderes ni cambios de
   ISR ni creación de topics.
2. Con 2 brokers se tolera **un** broker caído y sin copia extra durante la caída.
3. Reparto por hash de 8 tiendas en 4 particiones: 3/1/2/2.
4. Dedup limitado a 600 s; agregado con latencia de fin + 30 s.
5. Sin TLS/ACL en Kafka (aislado en la VPC).

**Mejora**: 3 brokers con KRaft (sin ZooKeeper), RF 3, `min.insync.replicas=2`.

---

## Preguntas típicas y respuestas cortas

| Pregunta | Respuesta |
|---|---|
| ¿Por qué no RF 3? | Solo hay 2 brokers: no existen 3 lugares distintos para las réplicas |
| ¿Qué pasa con `min.insync.replicas=2` si cae un broker? | El productor con `acks=all` recibe `NotEnoughReplicas`: se detiene la escritura (consistencia sobre disponibilidad) |
| ¿Por qué key = tiendaId? | La agregación es por tienda: misma partición → orden y estado en una sola tarea, sin repartition |
| ¿Qué es el stream time? | El mayor `eventTime` visto por la partición; decide cuándo cierra una ventana |
| ¿Por qué una fecha futura es inválida? | Adelantaría el stream time y cerraría ventanas que aún deberían recibir ventas |
| ¿EOS evita duplicados? | Evita reprocesar el **mismo offset**; no detecta la **misma venta** enviada dos veces (otro offset) |
| ¿Dónde vive el estado? | RocksDB local + topic changelog en Kafka; si se pierde el disco se reconstruye desde el changelog |
| ¿Cómo sé que no se perdió nada en la caída? | `conciliar`: ventas válidas únicas de raw = suma de agregadas por ventana, diferencia 0 |
| ¿Cómo reinyecto un mensaje de la DLQ? | Corregir `rawValue` y producirlo en `ventas-raw` con su key original; si ya se había aceptado, el `eventId` lo deduplica |
| ¿Qué pasa si cae ZooKeeper? | Brokers siguen con los líderes actuales; no hay elecciones ni cambios de ISR; es el SPOF del diseño |
| ¿Y la API Flask? | Servicio REST independiente en la EC2 app (:8081), con gunicorn y 1 worker porque los datos están en memoria; no participa del flujo Kafka |

## Al terminar

```bash
./verificar.sh apagar      # detiene las 4 EC2 sin perder datos (ahorra crédito del lab)
# ./verificar.sh destruir  # solo si ya no se necesita nada (borra todo)
```
