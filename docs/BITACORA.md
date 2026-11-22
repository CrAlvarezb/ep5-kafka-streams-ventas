# Bitácora de trabajo · EP5

Registro de lo que se hizo para construir, desplegar y probar la EP5, de los problemas encontrados y de cómo retomar el trabajo.

## 1. Punto de partida

- Encargo: Kafka (2 brokers + 1 ZooKeeper) y Kafka Streams en Java, sin Angular ni RabbitMQ, desplegado en **4 EC2 de AWS Academy**.
- Referencia: un ZIP con la entrega de otro equipo (informe, guía EP6 y 46 evidencias), **sin código fuente**. El repositorio que citaba no era accesible, así que todo se escribió desde cero siguiendo ese diseño.
- Cambio respecto de la referencia: la key es `tiendaId` desde el productor (la pauta pide clave por tienda) en vez de `customerId` con repartition.
- El curso entregó además una API Flask de usuarios (`app.py`), que se incorporó a la EC2 app.

## 2. Qué se construyó

| Pieza | Dónde |
|---|---|
| App Java 17: `producer`, `streams`, `casos`, `conciliar` | `app/` |
| 25 pruebas (validador + topología con `TopologyTestDriver`) | `app/src/test/` |
| Docker Compose por máquina | `deploy/kafka-1`, `deploy/kafka-2`, `deploy/app`, `deploy/monitoring` |
| API Flask del curso (gunicorn, :8081) | `deploy/app/api-usuarios/` |
| Terraform (VPC, 2 subredes en 2 AZ, 3 SG, 4 EC2 con bootstrap) | `infra/` |
| Creación e inspección de topics | `scripts/topicos.sh` |
| Operación y pruebas por SSM | `verificar.sh` |
| Plan B en un solo equipo | `local/docker-compose.local.yaml` |
| README, informe, guía de defensa, comandos, diagramas | `README.md`, `docs/` |
| Evidencias reales de AWS | `docs/evidencias/txt/` |
| Documentos Word | `documentos/` |
| Generador de los Word desde Markdown | `tools/md2docx.js` |

## 3. Despliegue en AWS (22-11-2026)

1. Se instaló Terraform con winget.
2. El usuario cargó las credenciales del Learner Lab con `aws configure` y `aws configure set aws_session_token …`.
3. `./verificar.sh desplegar` creó la VPC, los security groups y las 4 EC2. Las EC2 terminaron su bootstrap en unos 2 minutos.
4. `./verificar.sh subir-app` compiló, subió y levantó la app.
5. `./verificar.sh evidencias` ejecutó todos los escenarios y guardó las salidas.

## 4. Problemas encontrados y solución

| Problema | Causa | Solución |
|---|---|---|
| `InvalidClientTokenId` | el archivo de credenciales tenía claves viejas; el Bloc de notas guardó `credentials.txt` y se colaron líneas sobrantes | cargar las claves con `aws configure` |
| `terraform -chdir` no encontraba la carpeta | la ruta tiene un espacio ("cloud native") | `verificar.sh` entra a `infra/` con `cd` antes de llamar a Terraform |
| Error 403 al crear el bucket con Terraform | una política de Learner Lab niega `s3:GetBucketObjectLockConfiguration`, que el provider necesita | el bucket lo crea `verificar.sh` con AWS CLI y Terraform lo recibe como variable |
| Maven no arrancaba desde `verificar.sh` | `MSYS_NO_PATHCONV=1` rompe las rutas del lanzador de Maven en Git Bash | `mvn` se llama sin esas variables |
| El reporter de pruebas en árbol no cargaba | requería un plugin extra | se listan las pruebas desde los reportes XML de surefire |
| ZooKeeper se reiniciaba en bucle | el agente JMX no podía leer su archivo de reglas | configuración JMX mínima (exporta todo) |
| Aparecía un topic `…-repartition` | `process()` marca que la key podría cambiar | se usa `processValues()` (key fija) |
| Kafka Streams arrancó antes que los topics | `subir-app` corrió antes de que kafka-1 los creara | si Streams entra en `ERROR`, el proceso termina y Docker lo reinicia |
| Configuración de topics duplicada en la salida | `kafka-configs` imprime sinónimos | se muestra una línea por clave |
| Los `.sh` podían quedar con CRLF | `core.autocrlf` de Git en Windows | `.gitattributes` con `eol=lf` |

## 5. Resultados

- Los 9 casos controlados dieron el resultado esperado (DLQ con tipo, origen y original; `DUPLICADO`; tardío en gracia sumado; `TARDIO_FUERA_DE_GRACIA` en descartadas).
- Caída del broker 2: ISR `1,2` → `1` → `1,2`, producción y consumo sin errores.
- Conciliación: diferencia 0 en 20 ventanas, incluidas la caída y el reinicio.
- Reinicio de Kafka Streams sin estado: 5.100 registros restaurados y dedup persistente.
- Escalado a 2 instancias: particiones 2 y 2.
- 25 de 25 pruebas automatizadas OK. API Flask: CRUD y 404 OK.

Detalle completo: `docs/INFORME.md` y `documentos/Informe_EP5_Completo.docx`.

## 6. GitHub

- Repositorio público: https://github.com/CrAlvarezb/ep5-kafka-streams-ventas (rama `main`).
- Sin credenciales: `.gitignore` excluye `*.tfstate`, `*.env`, `*.pem`, `target/`.

## 7. Estado al cierre y cómo retomar

Las 4 EC2 quedaron **detenidas** (los datos se conservan en sus discos).

Para retomar:

1. Iniciar el Learner Lab y cargar las credenciales nuevas:
   ```bash
   aws configure
   aws configure set aws_session_token PEGA_AQUI_EL_TOKEN
   aws sts get-caller-identity
   ```
2. Encender todo:
   ```bash
   ./verificar.sh encender
   ```
   Comprobar en la salida: brokers `[1, 2]`, `Isr: 1,2` y `Kafka Streams: … -> RUNNING`. Si algo no levanta: `./verificar.sh estado`.
3. Si el lab se reseteó y las EC2 ya no existen: `./verificar.sh desplegar` y luego `./verificar.sh subir-app`.

El ciclo apagar → encender no se alcanzó a probar en esta sesión: revisar la salida la primera vez.

## 8. Pendientes

- Completar integrantes y docente en `docs/INFORME.md` y regenerar el Word:
  ```bash
  cd tools && npm install && cd ..
  node tools/md2docx.js documentos/Informe_EP5_Completo.docx "Informe EP5 · Streaming de ventas" "Informe, detalle técnico, guía de defensa, comandos y evidencias" docs/INFORME.md README.md docs/GUIA_PRUEBAS.md docs/COMANDOS.md
  node tools/md2docx.js documentos/Comandos_EP5_Explicados.docx "Comandos EP5 explicados" "Qué hace cada comando, dónde se ejecuta y qué se ve en la salida" docs/COMANDOS.md
  ```
- Capturas de Grafana y Kafka UI en `docs/evidencias/web/` (con las EC2 encendidas; URLs con `./verificar.sh urls`).
- Plan B local sin probar (Docker Desktop no arrancó en el PC).
