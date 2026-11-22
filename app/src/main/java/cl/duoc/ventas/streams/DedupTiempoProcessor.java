package cl.duoc.ventas.streams;

import cl.duoc.ventas.modelo.MotivoDescarte;
import cl.duoc.ventas.modelo.Venta;
import cl.duoc.ventas.modelo.VentaDescartada;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Etapa 2: deduplicacion de negocio y politica de tiempo, antes de agregar.
 * <ul>
 *   <li>DUPLICADO: el eventId ya fue aceptado (store dedup-event-id: window store RocksDB con
 *       changelog en Kafka y retencion configurable). No suma.</li>
 *   <li>TARDIO_FUERA_DE_GRACIA: fin de su ventana + gracia &lt;= stream time. La ventana ya cerro;
 *       se audita en ventas-descartadas en vez de perderse en silencio.</li>
 *   <li>TARDIO_EN_GRACIA: eventTime menor al stream time pero su ventana sigue abierta: se acepta y
 *       se suma a su ventana original (se registra en el log).</li>
 * </ul>
 * El eventId solo se guarda cuando la venta se acepta.
 */
public class DedupTiempoProcessor implements FixedKeyProcessor<String, Venta, Clasificacion> {

    private static final Logger LOG = LoggerFactory.getLogger("Tiempo");

    private final String storeName;
    private final long ventanaMs;
    private final long graciaMs;
    private final long retencionMs;
    private FixedKeyProcessorContext<String, Clasificacion> ctx;
    private WindowStore<String, String> vistos;

    public DedupTiempoProcessor(String storeName, Duration ventana, Duration gracia, Duration retencion) {
        this.storeName = storeName;
        this.ventanaMs = ventana.toMillis();
        this.graciaMs = gracia.toMillis();
        this.retencionMs = retencion.toMillis();
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Clasificacion> context) {
        this.ctx = context;
        this.vistos = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, Venta> r) {
        Venta v = r.value();
        long ts = v.eventTime().toEpochMilli();
        long streamTime = Math.max(ctx.currentStreamTimeMs(), ts);
        long inicio = Math.floorDiv(ts, ventanaMs) * ventanaMs;
        long fin = inicio + ventanaMs;

        RecordMetadata m = ctx.recordMetadata().orElse(null);
        String topic = m == null ? "?" : m.topic();
        int particion = m == null ? -1 : m.partition();
        long offset = m == null ? -1 : m.offset();
        String origen = topic + "-" + particion + "@" + offset;

        String previo = buscar(v.eventId(), ts);
        if (previo != null) {
            descartar(r, v, MotivoDescarte.DUPLICADO, "eventId ya aceptado en " + previo,
                    inicio, fin, streamTime, topic, particion, offset);
            return;
        }
        if (fin + graciaMs <= streamTime) {
            descartar(r, v, MotivoDescarte.TARDIO_FUERA_DE_GRACIA,
                    "la ventana cerro en " + Instant.ofEpochMilli(fin + graciaMs) + " y el stream time es "
                            + Instant.ofEpochMilli(streamTime) + " (atraso " + (streamTime - ts) / 1000 + " s)",
                    inicio, fin, streamTime, topic, particion, offset);
            return;
        }
        if (ts < streamTime) {
            LOG.info("TARDIO_EN_GRACIA eventId={} tienda={} eventTime={} streamTime={} atraso={}s ventana=[{} , {}) cierra={} -> se suma a su ventana original",
                    v.eventId(), v.tiendaId(), v.eventTime(), Instant.ofEpochMilli(streamTime), (streamTime - ts) / 1000,
                    Instant.ofEpochMilli(inicio), Instant.ofEpochMilli(fin), Instant.ofEpochMilli(fin + graciaMs));
        }
        vistos.put(v.eventId(), origen, ts);
        ctx.forward(r.withValue(Clasificacion.aceptada(v)).withTimestamp(ts));
    }

    private String buscar(String eventId, long ts) {
        try (WindowStoreIterator<String> it = vistos.fetch(eventId,
                Instant.ofEpochMilli(Math.max(0, ts - retencionMs)), Instant.ofEpochMilli(ts + retencionMs))) {
            return it.hasNext() ? it.next().value : null;
        }
    }

    private void descartar(FixedKeyRecord<String, Venta> r, Venta v, MotivoDescarte motivo, String detalle, long inicio, long fin,
                           long streamTime, String topic, int particion, long offset) {
        VentaDescartada d = new VentaDescartada(motivo, detalle, v.eventId(), v.tiendaId(), v.eventTime(),
                Instant.ofEpochMilli(inicio), Instant.ofEpochMilli(fin), Instant.ofEpochMilli(streamTime),
                topic, particion, offset, Instant.ofEpochMilli(ctx.currentSystemTimeMs()), v);
        LOG.warn("DESCARTE motivo={} eventId={} tienda={} eventTime={} ventana=[{} , {}) streamTime={} origen={}-{}@{} detalle=\"{}\"",
                motivo, v.eventId(), v.tiendaId(), v.eventTime(), Instant.ofEpochMilli(inicio), Instant.ofEpochMilli(fin),
                Instant.ofEpochMilli(streamTime), topic, particion, offset, detalle);
        ctx.forward(r.withValue(Clasificacion.descartada(d)));
    }
}
