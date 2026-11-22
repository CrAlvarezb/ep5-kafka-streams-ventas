package cl.duoc.ventas.streams;

import cl.duoc.ventas.Config;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Proceso Kafka Streams (servicio "streams" en Docker Compose). */
public final class StreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger("StreamsApp");

    private StreamsApp() {
    }

    public static Properties propiedades(Config cfg) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, cfg.applicationId());
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        // Transaccion que une: offsets consumidos + cambios de stores (changelog) + escrituras de salida.
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // Topics internos (changelog) con RF 2: hay 2 brokers.
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, cfg.replicationFactor());
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, cfg.streamThreads());
        p.put(StreamsConfig.STATE_DIR_CONFIG, cfg.stateDir());
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        // La entrada es String: no deberia fallar la deserializacion, pero si pasa no se detiene el flujo.
        p.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return p;
    }

    public static void ejecutar(Config cfg) throws InterruptedException {
        LOG.info("Configuracion: {}", cfg.resumen());
        Topology topologia = TopologiaVentas.construir(cfg);
        LOG.info("Topologia:\n{}", topologia.describe());

        KafkaStreams streams = new KafkaStreams(topologia, propiedades(cfg));
        streams.setStateListener((nuevo, anterior) -> {
            LOG.info("Kafka Streams: {} -> {}", anterior, nuevo);
            if (nuevo == KafkaStreams.State.ERROR) {
                // Sin threads vivos: se termina el proceso para que Docker lo reinicie (restart: unless-stopped)
                LOG.error("Kafka Streams en ERROR; se termina el proceso para que Docker lo reinicie");
                new Thread(() -> System.exit(1), "salida-error").start();
            }
        });
        streams.setGlobalStateRestoreListener(new RestauracionLog());
        streams.setUncaughtExceptionHandler(e -> {
            LOG.error("Excepcion no controlada en un StreamThread; se reemplaza el thread", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        CountDownLatch fin = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Cerrando Kafka Streams...");
            streams.close(Duration.ofSeconds(30));
            fin.countDown();
        }, "shutdown"));
        streams.start();
        fin.await();
    }

    /** Deja en el log cuanto estado se reconstruye desde cada changelog (evidencia de recuperacion). */
    static final class RestauracionLog implements StateRestoreListener {
        @Override
        public void onRestoreStart(TopicPartition tp, String store, long desde, long hasta) {
            LOG.info("RESTAURACION inicio store={} changelog={} offsets=[{}, {}] ({} registros por leer)", store, tp, desde, hasta, hasta - desde);
        }

        @Override
        public void onBatchRestored(TopicPartition tp, String store, long offsetLote, long registros) {
            // sin log por lote (demasiado ruido)
        }

        @Override
        public void onRestoreEnd(TopicPartition tp, String store, long total) {
            LOG.info("RESTAURACION fin store={} changelog={} registros restaurados={}", store, tp, total);
        }
    }
}
