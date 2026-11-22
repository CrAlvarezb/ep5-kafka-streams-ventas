package cl.duoc.ventas.modelo;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * Registro publicado en ventas-raw-dlq. Conserva el valor original sin tocar (rawValue), la key
 * original y el origen exacto (topic-particion@offset) para poder corregir y reinyectar.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RegistroDlq(
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        String originalKey,
        String rawValue,
        String eventId,
        TipoError errorType,
        String error,
        Instant failedAt,
        String applicationId) {
}
