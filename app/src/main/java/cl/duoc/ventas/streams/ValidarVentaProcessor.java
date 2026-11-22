package cl.duoc.ventas.streams;

import cl.duoc.ventas.modelo.RegistroDlq;
import cl.duoc.ventas.validacion.ValidadorVenta;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Etapa 1: valida cada registro de ventas-raw. Nunca lanza excepcion por datos malos: un registro
 * invalido se convierte en RegistroDlq (con el original intacto y su origen) y el flujo sigue.
 * Es un FixedKeyProcessor: no cambia la key (tiendaId), asi la agregacion no necesita repartition.
 */
public class ValidarVentaProcessor implements FixedKeyProcessor<String, String, Clasificacion> {

    private static final Logger LOG = LoggerFactory.getLogger("Validacion");

    private final ValidadorVenta validador;
    private final String applicationId;
    private FixedKeyProcessorContext<String, Clasificacion> ctx;

    public ValidarVentaProcessor(ValidadorVenta validador, String applicationId) {
        this.validador = validador;
        this.applicationId = applicationId;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Clasificacion> context) {
        this.ctx = context;
    }

    @Override
    public void process(FixedKeyRecord<String, String> r) {
        RecordMetadata m = ctx.recordMetadata().orElse(null);
        String topic = m == null ? "?" : m.topic();
        int particion = m == null ? -1 : m.partition();
        long offset = m == null ? -1 : m.offset();
        // La regla de "fecha futura" se evalua contra el reloj del procesador.
        Instant ahora = Instant.ofEpochMilli(ctx.currentSystemTimeMs());

        ValidadorVenta.Resultado res = validador.validar(r.key(), r.value(), ahora);
        if (res.valido()) {
            ctx.forward(r.withValue(Clasificacion.aceptada(res.venta())));
            return;
        }

        RegistroDlq dlq = new RegistroDlq(topic, particion, offset, r.key(), r.value(), res.eventId(),
                res.tipoError(), res.motivo(), ahora, applicationId);
        LOG.warn("DLQ tipo={} motivo=\"{}\" origen={}-{}@{} key={} eventId={}",
                res.tipoError(), res.motivo(), topic, particion, offset, r.key(), res.eventId());

        RecordHeaders h = new RecordHeaders(r.headers().toArray());
        h.add("dlq.error.type", bytes(res.tipoError().name()));
        h.add("dlq.error.reason", bytes(res.motivo()));
        h.add("dlq.source.topic", bytes(topic));
        h.add("dlq.source.partition", bytes(String.valueOf(particion)));
        h.add("dlq.source.offset", bytes(String.valueOf(offset)));
        ctx.forward(r.withValue(Clasificacion.aDlq(dlq)).withHeaders(h));
    }

    private static byte[] bytes(String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }
}
