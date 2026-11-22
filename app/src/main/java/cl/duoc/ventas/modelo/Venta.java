package cl.duoc.ventas.modelo;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * Evento de venta tipado (valor de ventas-raw). Key Kafka = tiendaId.
 * Montos en CLP enteros: evita errores de punto flotante al comparar total = cantidad x precio.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Venta(
        String eventId,
        String tiendaId,
        String tiendaNombre,
        Instant eventTime,
        String clienteId,
        String producto,
        String categoria,
        String canal,
        String medioPago,
        int cantidad,
        long precioUnitario,
        long total) {
}
