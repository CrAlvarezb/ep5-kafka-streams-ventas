# Informe EP5 · Streaming resiliente de ventas con Kafka Streams

| | |
|---|---|
| Asignatura | DSY1107 · Desarrollo Cloud Native I |
| Evaluación | Evaluación Parcial N° 5 · Encargo |
| Integrantes | _completar_ |
| Docente | _completar_ |
| Fecha | 22-11-2026 |

## 1. Resumen

Se construyó un flujo de ventas en tiempo real sobre un clúster Kafka de **dos brokers y un ZooKeeper**
desplegado con **Terraform en AWS Academy (4 EC2)**. Un productor Java publica ventas JSON con key
`tiendaId` en `ventas-raw`; una aplicación **Kafka Streams** valida cada venta, descarta duplicados por
`eventId` con un state store persistente, aplica una política explícita de eventos tardíos y agrupa por
tienda en **ventanas tumbling de 60 s por tiempo de evento con 30 s de gracia**, publicando al cierre
conteo, unidades, monto y ticket promedio en `ventas-agregadas`. Los datos inválidos se aíslan en
`ventas-raw-dlq` con el original intacto, su origen exacto y el motivo; duplicados y tardíos fuera de
gracia se auditan en `ventas-descartadas`. Se observa con Prometheus, Grafana, Loki y Kafka UI.

Todo se probó en AWS el 22-11-2026 (salida real en [evidencias/txt](evidencias/txt/)): los 9 casos
controlados dieron el resultado esperado; con el broker 2 detenido el ISR bajó a `1`, la producción
siguió (offset 1379 → 1476 → 1527) con **0 errores** y el broker volvió al ISR `1,2`; Kafka Streams se
eliminó sin estado local y reconstruyó **5.100 registros** desde sus changelog, reconociendo después
un reenvío como `DUPLICADO`. La conciliación entre `ventas-raw` y `ventas-agregadas` dio **diferencia 0
en 20 ventanas**, incluidas las de la caída y el reinicio. Las **25 pruebas automatizadas** pasan.

## 2. Arquitectura

- Flujo: [arquitectura.mmd](arquitectura.mmd) · Despliegue: [infraestructura.mmd](infraestructura.mmd)
- Detalle de topics, claves, configuración y decisiones: [README §4–§6](../README.md#4-modelo-de-eventos-topics-y-claves)

| EC2 | Tipo | IP privada | Contenedores |
|---|---|---|---|
| kafka-1 | t3.small | 10.20.1.10 (us-east-1a) | zookeeper, kafka (broker 1), grafana-alloy |
| kafka-2 | t3.small | 10.20.2.10 (us-east-1b) | kafka (broker 2), 4 lectores kcat, grafana-alloy |
| app | t3.small | 10.20.2.20 (us-east-1b) | producer, streams, api-usuarios, grafana-alloy |
| monitoring | t3.medium | 10.20.2.30 (us-east-1b) | prometheus, grafana, loki, kafka-exporter, kafka-ui, grafana-alloy |

## 3. Decisiones técnicas (resumen)

- **Key = `tiendaId`**: orden y estado por tienda en una sola tarea; sin repartition (`processValues`).
- **Tiempo de evento**: timestamp = `eventTime`; stream time = mayor `eventTime` visto por partición.
- **Ventana 60 s + gracia 30 s**: resumen por minuto; la gracia absorbe atrasos cortos; costo: el
  agregado sale al cierre (fin + 30 s) por `suppress`.
- **Inválido ≠ tardío**: inválido (no se puede usar) → DLQ para corregirlo; tardío (correcto pero
  atrasado) → `ventas-descartadas` para auditarlo.
- **Deduplicación**: store `dedup-event-id` con changelog, retención 600 s ≥ ventana + gracia.
- **`exactly_once_v2`** evita reprocesar el mismo offset tras un fallo, pero **no** detecta que el
  productor reenvió la misma venta (otro offset, mismo `eventId`): eso lo resuelve la deduplicación.
- **RF 2, `min.insync.replicas=1`, `unclean.leader.election=false`**, `transaction.state.log.min.isr=1`.

## 4. Manual de ejecución

Ver [README §3](../README.md#3-puesta-en-marcha-aws-desde-cero). Resumen:

```bash
./verificar.sh desplegar && ./verificar.sh subir-app && ./verificar.sh estado
./verificar.sh casos todos       # casos controlados + resultados
./verificar.sh fallo             # caída del broker 2 con producción
./verificar.sh conciliar 20      # sin pérdidas ni dobles conteos
./verificar.sh reinicio-streams  # restauración desde changelog
./verificar.sh evidencias        # todo lo anterior, guardado en docs/evidencias/txt/
```

## 5. Matriz de escenarios

| # | Escenario | Comando | Resultado esperado | Resultado obtenido | Evidencia |
|---|---|---|---|---|---|
| E1 | Levantamiento 4 EC2 | `desplegar`, `estado` | 4 EC2 running, BOOTSTRAP OK, contenedores arriba | Cumple: 4 running en 2 AZ, BOOTSTRAP OK en las 4 | 01, 02, 03 |
| E2 | Clúster y topics | `kafka`, `topicos` | brokers `[1, 2]`; 4 topics con particiones, RF 2, retención, min ISR 1; ISR `1,2` | Cumple; 0 sub-replicadas; líderes repartidos 1/2 | 04, 05 |
| E3 | Flujo normal | `app`, `flujo` | venta con key/partición/offset; agregado con inicio/fin, conteo, monto y ticket | Cumple: p. ej. T03 [17:33, 17:34) 13 ventas, $739.900, ticket $56.915; lag 0 | 06, 07, 08 |
| E4 | JSON malformado | `casos todos` | DLQ `JSON_MALFORMADO`, original intacto, Streams sigue | Cumple (`ventas-raw-1@111`) | 09 |
| E5 | Campo ausente / inválido | `casos todos` | DLQ `CAMPO_INVALIDO` | Cumple: sin eventTime (@112), cantidad "dos" (@113) | 09 |
| E6 | Total ≠ cantidad × precio | `casos todos` | DLQ `REGLA_NEGOCIO` | Cumple: 9999 ≠ 2 × 5000 (@114) | 09 |
| E7 | eventTime futuro | `casos todos` | DLQ `REGLA_NEGOCIO`, no adelanta stream time | Cumple (@116) | 09 |
| E8 | eventId repetido | `casos todos` | 1.ª suma; 2.ª `ventas-descartadas` `DUPLICADO` | Cumple: @118 DUPLICADO ("ya aceptado en ventas-raw-1@117") | 09 |
| E9 | Tardío dentro de gracia | `casos todos` | se suma a su ventana original | Cumple: atraso 15 s, `TARDIO_EN_GRACIA`, sumado | 09 |
| E10 | Tardío fuera de gracia | `casos todos` | `ventas-descartadas` `TARDIO_FUERA_DE_GRACIA`, no DLQ | Cumple: ventana [17:32, 17:33) cerró 17:33:30, stream time 17:35:51 | 09 |
| E11 | Agregado T-DEMO | `casos todos` | ventana con válida + duplicado 1 vez + anclas + tardío en gracia | Cumple: [17:35, 17:36) **5 ventas / $24.000** (sin dedup serían 6 / $34.000) | 09 |
| E12 | Offsets, lag, asignación, internos | `inspeccionar` | grupos con offset, fin, lag y consumidor; 3 changelog RF 2; sin repartition | Cumple | 10 |
| E13 | Broker 2 detenido | `fallo` | ISR `1,2` → `1` → `1,2`; producción y consumo siguen | Cumple: offset 1379 → 1476 → 1527 → 1619; 0 errores; 124 → 0 sub-replicadas | 11 |
| E14 | Sin pérdida silenciosa | `conciliar 30` | diferencia 0 por ventana | Cumple: 0 en 20 ventanas (incluye caída 17:39–17:41 y reinicio 17:42–17:43) | 12 |
| E15 | Reinicio de Streams sin estado | `reinicio-streams` | restaura desde changelog; continúa desde offsets; dedup persiste | Cumple: 5.100 registros restaurados; reenvío = `DUPLICADO`; lag 0 | 13 |
| E16 | Escalamiento | `escalar` | 4 particiones repartidas entre 2 instancias | Cumple: 2 y 2 | 14 |
| E17 | Observabilidad | `monitoreo` | targets up, Grafana ok, Loki recibe de 4 hosts, conteos por motivo | Cumple (9408 `down` es esperado: la 2.ª instancia ya se retiró) | 15 |
| E18 | Pruebas automatizadas | `pruebas` | todas OK | 25/25 OK | 16 |
| E19 | API de usuarios (Flask) | `api` | CRUD y 404 | Cumple | 17 |

## 6. Evidencias

Archivos en [evidencias/txt/](evidencias/txt/) (salida real de `./verificar.sh evidencias`, hora UTC):

| Archivo | Contenido |
|---|---|
| 01-ec2.txt | las 4 EC2 (tipo, AZ, IP privada fija) |
| 02-terraform.txt | recursos creados por Terraform |
| 03-estado.txt | BOOTSTRAP OK y contenedores por EC2 |
| 04-kafka.txt | brokers en ZooKeeper, topics, ISR |
| 05-topicos.txt | `scripts/topicos.sh`: creación idempotente, configuración, internos, grupos |
| 06-app.txt / 07-lectores.txt | productor (key, partición, offset), estado de Streams, lectores |
| 08-flujo.txt | ventas de entrada, agregados de salida y lag |
| 09-casos.txt | los 9 casos, DLQ (resumen + registro completo), descartadas, agregados y log |
| 10-inspeccionar.txt | topics, internos de Streams, consumer groups, offsets, lag y asignación |
| 11-fallo-broker.txt | ISR antes / durante / después de detener el broker 2 |
| 12-conciliacion.txt | raw vs agregados por ventana |
| 13-reinicio-streams.txt | restauración desde changelog y dedup persistente |
| 14-escalar.txt | reparto de particiones entre 2 instancias |
| 15-monitoreo.txt | Prometheus, Grafana, Loki y conteos por motivo |
| 16-pruebas.txt | 25 pruebas con su nombre |
| 17-api.txt | API REST de usuarios |

Capturas de Grafana y Kafka UI: agregar en `evidencias/web/` (URLs con `./verificar.sh urls`).

## 7. Límites conocidos y mejora

Ver [README §10](../README.md#10-límites-conocidos). El principal: **ZooKeeper en un solo nodo es un
punto único de falla** (además comparte EC2 con el broker 1). Con dos brokers solo se tolera la caída
de uno y durante ella cada escritura tiene una sola copia. Mejora priorizada: 3 brokers con KRaft,
RF 3 y `min.insync.replicas=2`.

## 8. Recuperación

Ver [README §11](../README.md#11-recuperación).
