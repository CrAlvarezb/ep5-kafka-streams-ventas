package cl.duoc.ventas.productor;

import cl.duoc.ventas.Config;
import cl.duoc.ventas.Json;
import cl.duoc.ventas.modelo.Venta;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Productor continuo de ventas validas (servicio "producer"). Key = tiendaId. */
public final class ProducerApp {

    private static final Logger LOG = LoggerFactory.getLogger("ProducerApp");

    private ProducerApp() {
    }

    /**
     * acks=all + idempotencia: el broker confirma solo cuando todas las replicas del ISR tienen el
     * registro, y los reintentos de red del cliente no crean duplicados. No evita que la aplicacion
     * reenvie la misma venta (eso lo resuelve la deduplicacion por eventId en Kafka Streams).
     */
    public static Properties propiedades(Config cfg, String clientId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        return p;
    }

    public static void ejecutar(Config cfg) throws InterruptedException {
        long pausaMs = Math.max(1, Math.round(1000 / cfg.ventasPorSegundo()));
        LOG.info("Productor: {} ventas/s hacia {} (acks=all, idempotencia, key=tiendaId)", cfg.ventasPorSegundo(), cfg.topicRaw());
        AtomicBoolean activo = new AtomicBoolean(true);
        AtomicLong enviadas = new AtomicLong();
        AtomicLong errores = new AtomicLong();

        try (KafkaProducer<String, String> productor = new KafkaProducer<>(propiedades(cfg, "ventas-producer"))) {
            Thread principal = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                activo.set(false);
                principal.interrupt();
            }));
            while (activo.get()) {
                Venta v = Catalogo.ventaAleatoria(Instant.now());
                String json = Json.escribir(v);
                productor.send(new ProducerRecord<>(cfg.topicRaw(), v.tiendaId(), json), (md, ex) -> {
                    if (ex != null) {
                        errores.incrementAndGet();
                        LOG.error("ERROR envio eventId={} tienda={}: {}", v.eventId(), v.tiendaId(), ex.toString());
                    } else {
                        long n = enviadas.incrementAndGet();
                        LOG.info("ENVIADA key={} particion={} offset={} eventId={} total={} n={}",
                                v.tiendaId(), md.partition(), md.offset(), v.eventId(), v.total(), n);
                    }
                });
                try {
                    Thread.sleep(pausaMs);
                } catch (InterruptedException e) {
                    break;
                }
            }
            productor.flush();
        }
        LOG.info("Productor detenido: enviadas={} errores={}", enviadas.get(), errores.get());
    }
}
