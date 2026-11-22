package cl.duoc.ventas.streams;

import cl.duoc.ventas.Json;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.OffsetDateTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Usa el eventTime de la venta como timestamp del registro: las ventanas agrupan por cuando ocurrio
 * la venta y no por cuando llego. El stream time de cada particion es el mayor eventTime visto.
 *
 * <p>Si el valor es invalido (no hay eventTime legible) o el eventTime esta demasiado en el futuro,
 * se usa el timestamp del registro (CreateTime del productor). Asi un dato basura nunca adelanta el
 * stream time ni cierra ventanas antes de tiempo; luego el validador lo envia a la DLQ.
 */
public class EventTimeExtractor implements TimestampExtractor {

    private final long futuroMaximoMs;

    public EventTimeExtractor(Duration futuroMaximo) {
        this.futuroMaximoMs = futuroMaximo.toMillis();
    }

    @Override
    public long extract(ConsumerRecord<Object, Object> registro, long partitionTime) {
        long respaldo = registro.timestamp() >= 0 ? registro.timestamp() : Math.max(partitionTime, 0);
        if (registro.value() instanceof String s) {
            try {
                JsonNode et = Json.MAPPER.readTree(s).get("eventTime");
                if (et != null && et.isTextual()) {
                    long ms = OffsetDateTime.parse(et.asText()).toInstant().toEpochMilli();
                    if (ms <= respaldo + futuroMaximoMs) {
                        return ms;
                    }
                }
            } catch (Exception ignorado) {
                // JSON roto o fecha invalida: se usa el respaldo y el validador lo manda a la DLQ
            }
        }
        return respaldo;
    }
}
