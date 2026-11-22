package cl.duoc.ventas.streams;

import cl.duoc.ventas.Config;
import cl.duoc.ventas.Json;
import cl.duoc.ventas.modelo.Acumulado;
import cl.duoc.ventas.modelo.Venta;
import cl.duoc.ventas.modelo.VentaAgregada;
import cl.duoc.ventas.validacion.ValidadorVenta;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Topologia:
 * <pre>
 * ventas-raw (key=tiendaId, timestamp=eventTime)
 *   -> validar-venta ──(invalido)──> ventas-raw-dlq   [original + key + topic/particion/offset + motivo]
 *   -> dedup-y-tiempo ─(duplicado / tardio fuera de gracia)──> ventas-descartadas
 *   -> ventana tumbling por tienda (store ventas-por-tienda)
 *   -> suppress hasta el cierre (store cierre-ventana-store)
 *   -> ventas-agregadas (inicio/fin, conteo, unidades, monto, ticket promedio)
 * </pre>
 * La key de ventas-raw ya es tiendaId y las etapas usan processValues (key fija), por lo que la
 * agregacion no necesita repartition: no hay topic interno -repartition, solo los 3 changelog.
 */
public final class TopologiaVentas {

    public static final String STORE_DEDUP = "dedup-event-id";
    public static final String STORE_AGREGADO = "ventas-por-tienda";
    public static final String SUPPRESS = "cierre-ventana";

    private static final Logger LOG = LoggerFactory.getLogger("Agregados");

    private TopologiaVentas() {
    }

    public static Topology construir(Config cfg) {
        Serde<String> s = Serdes.String();
        Serde<Venta> ventaSerde = Json.serde(Venta.class);
        Serde<Acumulado> acumuladoSerde = Json.serde(Acumulado.class);
        ValidadorVenta validador = new ValidadorVenta(cfg.futuroMaximo());

        StreamsBuilder b = new StreamsBuilder();
        b.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(STORE_DEDUP, cfg.retencionDedup(), cfg.retencionDedup(), false), s, s));

        KStream<String, String> raw = b.stream(cfg.topicRaw(),
                Consumed.with(s, s).withTimestampExtractor(new EventTimeExtractor(cfg.futuroMaximo())).withName("leer-ventas-raw"));

        // 1) Validacion -> DLQ
        Map<String, KStream<String, Clasificacion>> validacion = raw
                .processValues(() -> new ValidarVentaProcessor(validador, cfg.applicationId()), Named.as("validar-venta"))
                .split(Named.as("validacion-"))
                .branch((k, c) -> c.esDlq(), Branched.as("dlq"))
                .defaultBranch(Branched.as("ok"));

        validacion.get("validacion-dlq")
                .mapValues(c -> Json.escribir(c.dlq()), Named.as("dlq-a-json"))
                .to(cfg.topicDlq(), Produced.with(s, s).withName("escribir-dlq"));

        // 2) Deduplicacion por eventId + politica de tardios -> ventas-descartadas
        Map<String, KStream<String, Clasificacion>> tiempo = validacion.get("validacion-ok")
                .mapValues(Clasificacion::venta, Named.as("venta-tipada"))
                .processValues(() -> new DedupTiempoProcessor(STORE_DEDUP, cfg.ventana(), cfg.gracia(), cfg.retencionDedup()),
                        Named.as("dedup-y-tiempo"), STORE_DEDUP)
                .split(Named.as("tiempo-"))
                .branch((k, c) -> c.esDescarte(), Branched.as("descarte"))
                .defaultBranch(Branched.as("aceptada"));

        tiempo.get("tiempo-descarte")
                .mapValues(c -> Json.escribir(c.descarte()), Named.as("descarte-a-json"))
                .to(cfg.topicDescartadas(), Produced.with(s, s).withName("escribir-descartadas"));

        // 3) Ventana tumbling por eventTime + gracia; se publica solo al cerrar (suppress)
        tiempo.get("tiempo-aceptada")
                .mapValues(Clasificacion::venta, Named.as("venta-aceptada"))
                .groupByKey(Grouped.with(STORE_AGREGADO, s, ventaSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(cfg.ventana(), cfg.gracia()))
                .aggregate(Acumulado::vacio, (tienda, venta, acc) -> acc.sumar(venta), Named.as("agregar-ventana"),
                        Materialized.<String, Acumulado, WindowStore<Bytes, byte[]>>as(STORE_AGREGADO)
                                .withKeySerde(s)
                                .withValueSerde(acumuladoSerde)
                                .withRetention(cfg.retencionDedup()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName(SUPPRESS))
                .toStream(Named.as("ventanas-cerradas"))
                .map((w, acc) -> KeyValue.pair(w.key(),
                        VentaAgregada.de(w.key(), w.window().startTime(), w.window().endTime(), acc, Instant.now())),
                        Named.as("a-venta-agregada"))
                .peek((k, a) -> LOG.info("AGREGADO tienda={} ventana=[{} , {}) ventas={} unidades={} monto={} ticketPromedio={}",
                        k, a.windowStart(), a.windowEnd(), a.cantidadVentas(), a.unidades(), a.montoTotal(), a.ticketPromedio()),
                        Named.as("log-agregado"))
                .mapValues(Json::escribir, Named.as("agregado-a-json"))
                .to(cfg.topicAgregadas(), Produced.with(s, s).withName("escribir-agregadas"));

        return b.build();
    }
}
